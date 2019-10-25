# BJDB

A simple databases that only can save string.
一个迷你的行式数据库,目前只能存储字符串.

---

### Depends
Python3.*  

### Install Require
```
sudo make
cp config_example.py config.py
nano config.py
```

### Run
```
make run
```

### Use like a library

```Python
from bjdb import BJDB, Query

# New db
db = BJDB('test.db')
db.create_table('test', ['name', 'age'])

# Insert
db.insert('test', {'name': 'Tom', 'age': '3'})

# Search
query = Query()
results = list(db.search('test', query.name == 'Tom'))
print(results)

# Delete
db.delete('test', {'name': 'Tom', 'age': '3'})

# New table
db.create_table('phone_price', ['phone', 'price'])
db.insert('phone_price', {'phone':'iPhone X', 'price': '999'})
print(list( db.search('phone_price', query.phone == 'iPhone X')))

# Del table
db.purge('phone_price')

# Print all data from table
print(list(db.all('phone_price')))
```

### License (GPL-3.0)
