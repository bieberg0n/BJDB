import os
import csv


class BJDB:
    def __init__(self, filename, row_name=None):
        self.data = {'_default': []}
        self.tables = ['_default']
        self.default_table = '_default'

        if os.path.isfile(filename):
            self._readall(filename)

        else:
            self._create_db(filename, row_name)

    def _create_db(self, filename, row_name):
        row_whole = ['_id', '_table', '_status']
        row_whole.extend(row_name)

        f = open(filename, 'w')
        self.db_writer = csv.writer(f)
        self.db_dict_writer = csv.DictWriter(f, fieldnames=row_whole)
        self.db_dict_writer.writeheader()

        self.per_id = 0

    def _readall(self, filename):
        f = open(filename, 'r+')
        reader = csv.DictReader(f)
        for row in reader:
            if row.get('_table') in self.tables:
                self.data[row['_table']].append(row)
            else:
                self.data[row['_table']] = [row]
        self.per_id = int(row.get('_id')) + 1

    def init_writer(self):
        pass

    def insert(self, data):
        self.data[self.default_table].append(data)
        data['_id'] = self.per_id
        data['_table'] = self.default_table
        data['_status'] = 0
        self.db_dict_writer.writerow(data)
        self.per_id += 1

    def all(self):
        return self.data[self.default_table]


def test():
    db = BJDB('test.csv', ['uid', 'url'])
    # db.insert({'uid': 123, 'url': 'http://ip.cn'})
    # print(db.all())


test()
