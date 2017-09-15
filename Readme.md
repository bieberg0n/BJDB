# BJDB

一个迷你的行式数据库,目前只能存储字符串.

---

### Depends
Python3.*  
Tinydb

### Install
```
sudo pip3 install git+https://github.com/bieberg0n/bjdb.git
```

### Example Code
```Python
from bjdb import BJDB, Query

# New db
db = BJDB('test.db', ['name', 'age'])

# Insert
db['insert']({'name': 'Tom', 'age': '3'})

# Search
query = Query()
results = list(db['search'](query.name == 'Tom'))
print(results)

# Delete
db['delete']({'name': 'Tom', 'age': '3'})

# New table
db['create_table'](['phone', 'price'], 'phone_price')
db['insert']({'phone':'iPhone X', 'price': '999'}, table='phone_price')
print( list( db['search'](query.phone == 'iPhone X', table='phone_price') ) )

# Del table
db['purge']('phone_price')

# Print all data from table
print( list( db['all']('phone_price') ) )
```

### License (GPL-3.0)
