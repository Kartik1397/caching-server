import asyncio
import os
from enum import Enum

HOST = os.environ.get('HOST', 'localhost')
PORT = os.environ.get('PORT', 8000)

# key-value store
key_value_store = {}
key_tag_store = {}
tag_key_store = {}
MAX_KEYS = 100

class Operation(str, Enum):
    ECHO = 'ECHO'
    SET = 'SET'
    GET = 'GET'
    GETWITHTAGS = 'GETWITHTAGS'
    LISTKEYSWITHTAG = 'LISTKEYSWITHTAG'
    LOAD = 'LOAD'
    COUNT = 'COUNT'
    MAX = 'MAX'
    MIN = 'MIN'
    AVG = 'AVG'
    SUM = 'SUM'

def is_all_ints(values):
    for x in values:
        try:
            float(x)
        except:
            return False
    return True

def to_floats(values):
    return [float(x) for x in values]

def add(key, value, tags):
    if len(key_value_store.keys()) >= MAX_KEYS:
        return '!500'
    key_value_store[key] = value
    key_tag_store[key] = tags
    for tag in tags:
        if (tag in tag_key_store) == False:
            tag_key_store[tag] = []
        tag_key_store[tag].append(key)
    return '!200'

def get(key):
    if key in key_value_store:
        return key_value_store[key]
    else:
        return '!404'

def getwithtags(key):
    if key in key_value_store:
        return key_value_store[key] + ' [' + ' '.join(key_tag_store[key]) + ']'
    else:
        return '!404'

def getlistkeyswithtag(tag):
    if tag in tag_key_store:
        return '[' + ' '.join(tag_key_store[tag]) + ']'
    else:
        return '!404'

def load(file_name):
    try:
        with open(file_name, 'r') as db_file:
            lines = db_file.readlines()
            for line in lines:
                line = line.strip()
                if len(line) <= 0:
                    continue
                operands = line.split(' ', 2)
                if len(operands) < 2:
                    return '!500'
                if len(operands) == 2:
                    res = add(operands[0].strip(), operands[1].strip(), [])
                else:
                    res = add(operands[0].strip(), operands[1].strip(), operands[2].split())
                if res == '!500':
                    return '!500'
            return '!200'
    except Exception as error:
        print(error)
        return '!404'

def agg(op, tag):
    if (tag in tag_key_store) == False:
        return '!404'
    values = [key_value_store[key] for key in tag_key_store[tag]]
    if op == Operation.COUNT:
        return str(len(values))
    elif op == Operation.MAX:
        if is_all_ints(values):
            return str(max(to_floats(values)))
        else:
            return '!400'
    elif op == Operation.MIN:
        if is_all_ints(values):
            return str(min(to_floats(values)))
        else:
            return '!400'
    elif op == Operation.AVG:
        if is_all_ints(values):
            return str(sum(to_floats(values))/len(values))
        else:
            return '!400'
    elif op == Operation.SUM:
        if is_all_ints(values):
            return str(sum(to_floats(values)))
        else:
            return '!400'
    else:
        return '!400'

# read write helper async def write_result(writer, data):
async def write_result(writer, data):
    writer.write(data.encode('ascii'))
    await writer.drain()

async def read_instruction(reader):
    instruction = await reader.readline()
    return instruction.decode('ascii')

# instruction executer
async def handle_echo(writer, data):
    await write_result(writer, data)

async def handle_set(writer, key, value, tags=[]):
    res = add(key, value, tags)
    await write_result(writer, res)

async def handle_get(writer, key):
    data = get(key)
    await write_result(writer, data)

async def handle_getwithtags(writer, key):
    data = getwithtags(key)
    await write_result(writer, data)

async def handle_listkeyswithtag(writer, tag):
    data = getlistkeyswithtag(tag)
    await write_result(writer, data)

async def handle_load(writer, file_name):
    data = load(file_name)
    await write_result(writer, data)

async def handle_agg(writer, op, tag):
    data = agg(op, tag)
    await write_result(writer, data)

async def handle_flush(writer):
    await write_result(writer, 'abc')

async def handle_syntax_error(writer):
    await write_result(writer, '!400')

# instruction processor
async def process(writer, instruction):
    instruction.strip()
    tokens = instruction.split(' ', 1)
    operation = tokens[0].strip()
    if operation == Operation.ECHO:
        operand = tokens[1]
        await handle_echo(writer, operand.strip())
    elif operation == Operation.SET:
        if len(tokens) < 2:
            await handle_syntax_error(writer)
            return
        operands = tokens[1].split(' ', 2)
        if len(operands) < 2:
            await handle_syntax_error(writer)
            return
        if len(operands) == 2:
            await handle_set(writer, operands[0].strip(), operands[1].strip())
        else:
            await handle_set(writer, operands[0].strip(), operands[1].strip(), operands[2].split())
    elif operation == Operation.GET:
        if len(tokens) < 2:
            await handle_syntax_error(writer)
            return
        operand = tokens[1].split(' ', 1)[0]
        await handle_get(writer, operand.strip())
    elif operation == Operation.GETWITHTAGS:
        if len(tokens) < 2:
            await handle_syntax_error(writer)
            return
        operand = tokens[1].split(' ', 1)[0]
        await handle_getwithtags(writer, operand.strip())
    elif operation == Operation.LISTKEYSWITHTAG:
        if len(tokens) < 2:
            await handle_syntax_error(writer)
            return
        operands = tokens[1].split(' ')
        if len(operands) < 2 or operands[0] != 'EXACT':
            await handle_syntax_error(writer)
            return
        await handle_listkeyswithtag(writer, operands[1].strip())
    elif operation == Operation.LOAD:
        if len(tokens) < 2:
            await handle_syntax_error(writer)
            return
        operand = tokens[1].split(' ', 1)[0]
        await handle_load(writer, operand.strip())
    elif (
        operation == Operation.COUNT or
        operation == Operation.MAX or
        operation == Operation.MIN or
        operation == Operation.AVG or
        operation == Operation.SUM
    ):
        if len(tokens) < 2:
            await handle_syntax_error(writer)
            return
        operands = tokens[1].split(' ')
        if len(operands) < 2 or operands[0] != 'TAG':
            await handle_syntax_error(writer)
            return
        await handle_agg(writer, operation, operands[1].strip())
    elif operation == 'FLUSH':
        await handle_flush(writer)
    else:
        await handle_syntax_error(writer)

# handle new client connection
async def handle_client(reader, writer):
    while True:
        instruction = await read_instruction(reader)
        if len(instruction) == 0:
            break
        await process(writer, instruction)

async def main(host, port):
    server = await asyncio.start_server(handle_client, host, port)
    print(f"Listening on {host}:{port}")
    await server.serve_forever()

asyncio.run(main(HOST, PORT))
