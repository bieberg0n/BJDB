import os
import struct
from tinydb import Query


def pack_data(arg):
    # bytes长度+bytes

    if isinstance(arg, bytes):
        bytes_ = arg
    else:
        bytes_ = str(arg).encode()

    len_bytes = struct.pack('>H', len(bytes_))
    return len_bytes + bytes_


def to_header_bytes(headers, table='_default'):
    # 生成headers record

    line_type = b't'

    headers_whole = [table]
    headers_whole.extend(headers)

    headers_bytes = b''.join([pack_data(i) for i in headers_whole])

    return pack_data(line_type + headers_bytes)


def list_to_record(table, data_list, record_type=b'i'):
    # list -> record

    _data_list = (table, ) + data_list
    data_bytes = b''.join([pack_data(d) for d in _data_list])
    return pack_data(record_type + data_bytes)


def dict_to_record(table, _id, headers, data, record_type=b'i'):
    # dict -> Record

    data_list = [data[h] for h in headers if data and data.get(h)]
    return list_to_record(table, data_list, record_type)


def get_empty_db():
    db = {
        '_default': {
            'headers': [],
            'datas': set()
        }
    }
    return db


def create_db(filename, headers):
    writer = open(filename, 'wb')
    record = to_header_bytes(headers)
    write(writer, record)

    db = get_empty_db()
    db['_default']['headers'] = headers

    return db, writer


def unpack_record(f):
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
    return tuple(data_list)


def read_db(filename):
    # data_bytes[0]: record type
    # data_list[0]: table name
    # data_list[1:]: data

    f = open(filename, 'r+b')
    db = get_empty_db()

    for data_bytes in iter(lambda: unpack_record(f), b''):

        data_list = unpack_data(data_bytes[1:])
        table = data_list[0]
        data = data_list[1:]

        if data_bytes[:1] == b't':
            if not db.get(table):
                db[table] = {'headers': [], 'datas': set()}
            db[table]['headers'] = list(data_list[1:])

        elif data_bytes[:1] == b'i':
            db[table]['datas'].add(data)

        elif data_bytes[:1] == b'd':
            db[table]['datas'].remove(data)

        elif data_bytes[:1] == b'u':
            db[table]['datas'][int(data_list[1])] = data_list[1:]

        elif data_bytes[:1] == b'p':
            db[table]['datas'] = set()

    return db, f


def to_dict(headers, element):
    if element:
        return dict(zip(headers, element))
    else:
        return element


def write(writer, bytes_):
    writer.write(bytes_)
    writer.flush()


class BJDB:
    def __init__(self, filename, header=None):
        self.filename = filename
        if os.path.isfile(filename):
            self.db, self.writer = read_db(filename)

        else:
            self.db, self.writer = create_db(filename, header)

    def insert(self, data, table='_default'):
        data_tuple = tuple(str(data[h]) for h in self.db[table]['headers'])
        record = list_to_record(table,
                                data_tuple)
        self.db[table]['datas'].add(data_tuple)
        write(self.writer, record)

    def exist(self, data, table='_default'):
        data_tuple = tuple(str(data[h]) for h in self.db[table]['headers'])
        return data_tuple in self.db[table]['datas']

    def search(self, cond, table='_default'):
        headers = self.db[table]['headers']
        elements_dict = (to_dict(headers, e) for e in self.db[table]['datas'] if e)

        results = (e for e in elements_dict if cond(e))
        return results

    def update_element(self, table, _id, olddata, newdata, record_type=b'u'):
        if record_type == b'u':
            olddata.update(newdata)
            newdata_list = [olddata[h] for h in self.db[table]['headers']]
        else:
            newdata_list = []
        self.db[table]['datas'][_id] = newdata_list

        record = list_to_record(table=table,
                                # _id=_id,
                                data_list=newdata_list,
                                record_type=record_type)
        write(self.writer, record)

    def delete_element(self, table, _id):
        self.update_element(table, _id, {}, {}, b'd')

    def delete(self, data_dict, table='_default'):
        headers = self.db[table]['headers']
        element = tuple(data_dict[h] for h in headers)
        self.db[table]['datas'].remove(element)

        write(self.writer, list_to_record(table, element, record_type=b'd'))

    def update(self, newdata, cond, table='_default'):
        headers = self.db[table]['headers']
        elements_dict = (to_dict(headers, e) for e in self.db[table]['datas'])

        [self.update_element(table, i, olddata, newdata)
         for i, olddata in enumerate(elements_dict) if cond(olddata)]

    def merge(self):
        self.writer.close()
        temp_file = self.filename + '~'
        with open(temp_file, 'wb') as f:
            for table in self.db:
                headers = self.db[table]['headers']
                f.write(to_header_bytes(headers))
                for data in self.db[table]['datas']:
                    if data:
                        record = list_to_record(table=table,
                                                # _id=_id,
                                                data_list=data)
                        f.write(record)
                    else:
                        pass
        os.rename(temp_file, self.filename)

    def create_table(self, headers, table_name='_default'):
        self.db[table_name] = {
            'headers': headers,
            'datas': set()
        }
        self.writer.write(to_header_bytes(headers, table_name))

    def purge(self, table='_default'):
        self.db[table]['datas'] = set()
        self.writer.write(list_to_record(table, tuple(), b'p'))

    def all(self, table='_default'):
        headers = self.db[table]['headers']
        return (to_dict(headers, e) for e in self.db[table]['datas'])

    def tables(self):
        return list(self.db.keys())

    def headers(self, table='_default'):
        return self.db[table]['headers']


def test1():
    filename = 'test.db'

    db = BJDB(filename, ['url'])
    print('初始化数据库')
    print(list(db.all()))

    db.insert({'url': 'http://example1.com'})
    db.insert({'url': 'http://example2.com'})
    # db.insert({'url': 'http://example3.com'})
    # print('测试插入')
    # print(list(db.all()))
    # os.remove(filename)

    e = Query()
    print('搜索c', list(db.search(e.url == 'http://example1.com')))
    print('是否存在', db.exist({'url': 'http://example2.com'}))
    print('是否存在', db.exist({'url': 'http://example3.com'}))

    # db.update({'url': 'http://ip.cn'}, e.uid == 'a')
    # print('测试修改', db.all())

    # db.delete(e.uid == 'b')
    # db.delete(e.uid == 'c')
    db.delete({'url': 'http://example2.com'})
    print('测试删除')
    print(list(db.all()))

    db = BJDB(filename)
    print(list(db.all()))

    db.merge()
    print(list(db.all()))

    print(db.tables())

    os.remove(filename)


def test2():
    filename = 'test.db'

    db = BJDB(filename, ['url'])
    print('初始化数据库')
    # print(list(db['all']()))
    db.insert({'url': 'http://example1.com'})
    print(list(db.all()))

    os.remove(filename)


if __name__ == '__main__':
    test1()

