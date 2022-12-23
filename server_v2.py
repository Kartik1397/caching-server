import asyncio

# key-value store
key_value_store = {}
MAX_KEYS = 3

def add(key, value):
    if len(key_value_store) == MAX_KEYS:
        return '!500'
    key_value_store[key] = value
    return '!200'

def get(key):
    if key in key_value_store:
        return key_value_store[key]
    else
        return '!404'

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

async def handle_set(writer, key, value):
    add(key, value)
    await write_result(writer, '!200')

async def handle_get(writer, key):
    data = get(key)
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
    if operation == 'ECHO':
        operand = tokens[1]
        await handle_echo(writer, operand.strip())
    elif operation == 'SET':
        if len(tokens) < 2:
            await handle_syntax_error(writer)
        operands = tokens[1].split(' ', 1)
        if len(operands) < 2:
            await handle_syntax_error(writer)
        await handle_set(writer, operands[0].strip(), operands[1].strip())
    elif operation == 'GET':
        if len(tokens) < 2:
            await handle_syntax_error(writer)
        operand = tokens[1].split(' ', 1)[0]
        await handle_get(writer, operand.strip())
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
    await server.serve_forever()

asyncio.run(main('localhost', 8000))
