import os
import struct
from tinydb import Query


def pack_data(arg):
    # if isinstance(arg, str):
    #     bytes_ = arg.encode()
    if isinstance(arg, bytes):
        bytes_ = arg
    else:
        bytes_ = str(arg).encode()

    len_bytes = struct.pack('>H', len(bytes_))
    return len_bytes + bytes_


def to_header_bytes(header):

    line_type = b't'

    header_whole = ['_default', '_id']
    header_whole.extend(header)

    header_bytes = b''.join([pack_data(i) for i in header_whole])

    return pack_data(line_type + header_bytes)


def to_record(table, _id, headers, data, record_type=b'i'):

    ''' 生成一条数据Record'''

    # _id_byte = str(_id).encode()

    data_list = [table, _id] + [data[h] for h in headers if data.get(h)]
    data_bytes = b''.join([pack_data(d) for d in data_list])

    return pack_data(record_type + data_bytes)


def get_empty_db():
    db = {
        '_default':{
            'headers':[],
            'data':[]
        }
    }
    return db


def create_db(filename, header):
    writer = open(filename, 'wb')
    writer.write(to_header_bytes(header))

    db = get_empty_db()
    db['_default']['headers'] = header

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
        print(data_list)
        table = data_list[0]

        if record[:1] == b't':
            if not db.get(table):
                db[table] = []
            db[table]['headers'] = data_list[2:]

        elif record[:1] == b'i':
            data = data_list[2:]
            db[table]['data'].append(data)

        elif record[:1] == b'd':
            db[table]['data'][int(data_list[1])] = None

        elif record[:1] == b'u':
            db[table]['data'][int(data_list[1])] = data_list[2:]

        else:
            pass

    return db, f


def to_dict(headers, element):
    return dict(zip(headers, element))


def write(writer, bytes_):
    writer.write(bytes_)
    writer.flush()


def bjdb(filename, header=None):
    if os.path.isfile(filename):
        db, writer = read_db(filename)

    else:
        db, writer = create_db(filename, header)

    def insert(data, table='_default'):
        data_list = [data[h] for h in db[table]['headers']]
        record = to_record(table,
                           _id=len(db[table]['data']),
                           headers=db[table]['headers'],
                           data=data)
        db[table]['data'].append(data_list)
        write(writer, record)
        print(db)

    def search(cond, table='_default'):

        headers = db[table]['headers']
        elements_dict = (to_dict(headers, e) for e in db[table]['data'] if e)

        results = [e for e in elements_dict if cond(e)]
        return results

    def delete(cond, table='_default'):

        headers = db[table]['headers']
        elements_dict = (to_dict(headers, e) for e in db[table]['data'])

        for i, e in enumerate(elements_dict):
            if cond(e):
                db[table]['data'][i] = None

                record = to_record(table=table,
                                   _id=i,
                                   headers=headers,
                                   data={},
                                   record_type=b'd')
                # print(record)
                write(writer, record)

            else:
                pass

        print(db)

    def update(newdata, cond, table='_default'):

        headers = db[table]['headers']
        elements_dict = (to_dict(headers, e) for e in db[table]['data'])

        for i, olddata in enumerate(elements_dict):
            if cond(olddata):
                olddata.update(newdata)
                newdata_list = [olddata[h] for h in headers]
                db[table]['data'][i] = newdata_list
                record = to_record(table=table,
                                   _id=i,
                                   headers=headers,
                                   data=olddata,
                                   record_type=b'u')
                write(writer, record)
            else:
                pass
        print(db)


    method = {
        'insert': insert,
        'search': search,
        'delete': delete,
        'update': update
    }
    return method


def test():
    db = bjdb('test.db', ['uid', 'url'])
    # db['insert']({'uid': 'a', 'url': 'http://example.com'})
    e = Query()
    print(db['search'](e.uid == 'd'))
    # db['update']({'url': 'http://ip.cn'}, e.uid == 'a')
    # db['delete'](e.uid == 'a')


test()

