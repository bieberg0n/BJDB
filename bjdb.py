import os
import struct


def pack_data(arg):
    if isinstance(arg, str):
        bytes_ = arg.encode()
    elif isinstance(arg, bytes):
        bytes_ = arg
    else:
        bytes_ = str(arg).encode()

    len_bytes = struct.pack('>H', len(bytes_))
    return len_bytes + bytes_


def to_header_bytes(header):
    line_type = b't'
    header_whole = ['_default', '_id', '_status']
    header_whole.extend(header)
    header_bytes = b''.join([pack_data(i) for i in header_whole])
    return pack_data(line_type + header_bytes)


def to_data_bytes(table, _id, header, data, status=0):
    ''' 生成一条数据Record'''
    line_type = b'd'
    _id_byte = struct.pack('>H', _id)
    data_list = [table, _id_byte, status] + [data[h] for h in header]
    data_bytes = b''.join([pack_data(d) for d in data_list])
    return pack_data(line_type + data_bytes)


def get_empty_db():
    db = {
        '_default':[],
        '_headers':{
            '_default': []
        }
    }
    return db


def create_db(filename, header):
    writer = open(filename, 'wb')
    writer.write(to_header_bytes(header))

    db = get_empty_db()
    db['_headers']['_default'] = header

    return db, writer


def get_record(f):
    r = f.read(2)
    if r:
        length = struct.unpack('>H', r)[0]
        r = f.read(length)
        return r
    else:
        return b''


def unpack_data(pdata):
    data_list = []
    while pdata:
        len_b, pdata = pdata[:2], pdata[2:]
        length = struct.unpack('>H', len_b)[0]
        data_list.append(pdata[:length].decode())
        pdata = pdata[length:]
    return data_list


def read_db(filename):
    f = open(filename, 'r+b')
    db = get_empty_db()

    for record in iter(lambda: get_record(f), b''):
        data_list = unpack_data(record[1:])
        table = data_list[0]
        if record[:1] == b't':
            db['_headers'] = {data_list[0]: data_list[3:]}
            if not db.get(table):
                db[table] = []

        elif record[:1] == b'd':
            data = dict(zip(db['_headers'][table], data_list[3:]))
            db[table].append(data)

        else:
            pass

    return db, f


def bjdb(filename, header=None):
    if os.path.isfile(filename):
        db, writer = read_db(filename)

    else:
        db, writer = create_db(filename, header)

    def insert(db, data, table='_default'):
        db[table].append(data)
        writer.write(to_data_bytes(table, len(db[table]), db['_headers'][table], data))

    return db, insert


def test():
    db, insert = bjdb('test.db', ['uid', 'url'])
    print(db)
    insert(db, {'uid': 'abc', 'url': 'http://example.com'})
    print(db)
    # print(db.all())


test()

