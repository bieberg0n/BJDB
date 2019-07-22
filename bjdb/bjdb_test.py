import os
from bjdb import BJDB
from tinydb import Query


def test1():
    filename = 'test.db'

    db = BJDB(filename)
    print('初始化数据库')
    db.create_table('urls', ['url', 'd'])
    print(db.all('urls'))

    print('测试插入')
    db.insert('urls', {'url': 'http://example1.com', 'd': '1'})
    db.insert('urls', {'url': 'http://example2.com', 'd': '2'})
    db.insert('urls', {'url': 'http://example3.com', 'd': '3'})
    print(db.all('urls'))
    # os.remove(filename)

    e = Query()
    print('搜索c', list(db.search('urls', e.url == 'http://example1.com')))
    print('是否存在', db.exist('urls', e.url == 'http://example2.com'))
    print('是否存在', db.exist('urls', e.url == 'http://example3.com'))

    print('测试修改')
    db.update('urls', {'url': 'http://ip.cn'}, e.d == '2')
    print(db.all('urls'))

    print('测试删除')
    # db.delete(e.uid == 'b')
    # db.delete(e.uid == 'c')
    db.delete('urls', e.d == '3')
    print(db.all('urls'))
    db.close()

    db = BJDB(filename)
    print(db.all('urls'))

    print('测试合并')
    db.merge()
    print(db.all('urls'))

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
