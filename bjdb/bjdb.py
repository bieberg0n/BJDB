import os
import struct
import pprint


def log(*args):
    if len(args) == 1:
        pprint.pprint(*args)
    else:
        print(*args)


def pack_data(bytes_data):
    # bytes长度+bytes

    length = struct.pack('>H', len(bytes_data))
    return length + bytes_data


def header_to_bytes(table, headers):
    # 生成headers record

    line_type = b't'
    headers = [table] + headers
    headers_bytes = b''.join([pack_data(i.encode()) for i in headers])
    return pack_data(line_type + headers_bytes)


def tuple_to_record(table, record_type, data_tuple):
    # list -> record

    data_tuple = (table,) + data_tuple
    data_bytes = b''.join([pack_data(d.encode()) for d in data_tuple])
    return pack_data(record_type.encode() + data_bytes)


def create_db(filename):
    f = open(filename, 'wb')
    return {}, f


def unpack_record(f):
    r = f.read(2)
    if r:
        length = struct.unpack('>H', r)[0]
        r = f.read(length)

    return r


def unpack_data(pdata):
    data_list = []
    while pdata:
        len_b, pdata = pdata[:2], pdata[2:]
        length = struct.unpack('>H', len_b)[0]
        data_list.append(pdata[:length].decode())
        pdata = pdata[length:]
    return tuple(data_list)


class DataReader:
    def __init__(self, data):
        self.data = data
        self.offset = 0

    def read(self, n):
        tail = self.offset + n

        if self.offset == len(self.data):
            return b''
        elif tail > len(self.data):
            return self.data[self.offset:]
        else:
            d = self.data[self.offset:tail]
            self.offset = tail
            return d


def get_db(filename):
    # data_bytes[0]: record type
    # data_list[0]: table name
    # data_list[1:]: data

    if not os.path.isfile(filename):
        return create_db(filename)

    f = open(filename, 'r+b')
    reader = DataReader(f.read())
    db = {}

    for record in iter(lambda: unpack_record(reader), b''):
        # 若使用record[0]会返回int
        record_type = record[:1].decode()
        data_tuple = unpack_data(record[1:])
        table = data_tuple[0]
        data = data_tuple[1:]

        if record_type == 't':
            if db.get(table):
                db[table]['headers'] = list(data)
            else:
                db[table] = {'headers': list(data), 'datas': set()}

        elif record_type == 'i':
            db[table]['datas'].add(data)

        elif record_type == 'd':
            db[table]['datas'].remove(data)

        elif record_type == 'u':
            mid = len(data) // 2
            db[table]['datas'].remove(data[:mid])
            db[table]['datas'].add(data[mid:])

        elif record_type == 'p':
            db[table]['datas'] = set()

    return db, f


def to_dict(headers, element):
    if element:
        return dict(zip(headers, element))
    else:
        return element


class BJDB:
    def __init__(self, filename):
        self.filename = filename
        self.db, self.writer = get_db(filename)

    def write(self, table, record_type, data_list):
        r = tuple_to_record(table, record_type, data_list)
        self.writer.write(r)
        self.writer.flush()

    def dict_to_tuple(self, table, dic):
        return tuple(str(dic.get(h)) for h in self.db[table]['headers'])

    def insert(self, table, data):
        data_tuple = self.dict_to_tuple(table, data)
        self.db[table]['datas'].add(data_tuple)
        self.write(table, 'i', data_tuple)

    def search(self, table, cond):
        headers = self.db[table]['headers']
        dic = lambda e: to_dict(headers, e)
        return [dic(e) for e in self.db[table]['datas'] if cond(dic(e))]

    def exist(self, table, cond):
        return len(self.search(table, cond)) > 0

    def delete(self, table, cond):
        headers = self.db[table]['headers']
        elements = [e for e in self.db[table]['datas'] if cond(to_dict(headers, e))]
        for e in elements:
            self.db[table]['datas'].remove(e)
            self.writer.write(tuple_to_record(table, 'd', e))

        self.writer.flush()

    def update(self, table, newdata, cond):
        headers = self.db[table]['headers']
        dic = lambda e: to_dict(headers, e)
        elements = (e for e in self.db[table]['datas'] if cond(dic(e)))
        for e in elements:
            new_d = dic(e)
            new_d.update(newdata)
            newdata_tuple = self.dict_to_tuple(table, new_d)
            self.db[table]['datas'].remove(e)
            self.db[table]['datas'].add(newdata_tuple)
            self.writer.write(tuple_to_record(table, 'u', e + newdata_tuple))

        self.writer.flush()

    def merge(self):
        self.writer.close()
        temp_file = self.filename + '~'
        with open(temp_file, 'wb') as f:
            for table in self.db:
                headers = self.db[table]['headers']
                f.write(header_to_bytes(table, headers))
                for data in self.db[table]['datas']:
                    if data:
                        record = tuple_to_record(table, 'i', data)
                        f.write(record)
        os.rename(temp_file, self.filename)

    def create_table(self, table_name, headers):
        self.db[table_name] = {
            'headers': headers,
            'datas': set()
        }
        self.writer.write(header_to_bytes(table_name, headers))
        self.writer.flush()

    def purge(self, table):
        self.db[table]['datas'] = set()
        self.write(table, 'p', tuple())

    def all(self, table):
        headers = self.db[table]['headers']
        return [to_dict(headers, e) for e in self.db[table]['datas']]

    def tables(self):
        return list(self.db.keys())

    def headers(self, table):
        return self.db[table]['headers']

    # def table(self, table_name):
    #     return Table(self, table_name)

    def close(self):
        self.writer.close()


# class Table:
#     def __init__(self, bjdb: BJDB, table_name):
#         self.db = bjdb,
#         self.table_name = table_name
#
#     def insert(self, data):
#         self.db.insert(self.table_name, data)
#
#     def exist(self, data):
#         self.db.exist(self.table_name, data)
#
#     def search(self, data, cond):
#         self.db.search(self.table_name, data, cond)
#
#     def delete(self, data):
#         self.db.delete(self.table_name, data)
#
#     def all(self):
#         self.db.all(self.table_name)