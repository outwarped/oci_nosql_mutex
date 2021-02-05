# Description

Distributed Mutex implementation with OCI (Oracle Cloud Infrastructure) [NoSQL API](https://oracle-cloud-infrastructure-python-sdk.readthedocs.io/en/latest/api/nosql.html)

## NoSQL table schema

|Primary key|Column name|Type|Shard key|Not null|
|---|---|---|---|---|
|Yes|key|STRING|Yes|Yes|
|No|score|NUMBER|No|Yes|
|No|owner|STRING|No|No|
|No|body|JSON|No|No|

## Key points

* Mutex is represented as a single record in NoSQL table.

* Mutex record has a payload body

* Mutex lock is performed by updating record's score (timestamp).

* NoSQL `etag` (record version) is used to control Mutex ownership. When Mutex record is updated old `etag` is checked to match current `etag`, thus esuring there were no updates on the record.

* To acquire mutex the corresponding record's score is checked to be _less than_ current timestamp minus mutex timeout delta (default 60s) and then record's score is updated with current timestamp. `etag` is returned to be used in future updates.

* To acquire any mutex first record in the score range from 0 to _less than_ current timestamp minus mutex timeout delta are checked. Then record's score is updated with current timestamp
  
* Valid acquired or updated mutex is returned as a tuple of mutex name and its `etag` (string, string).

* Only corresponding record's `etag` is checked to match the old `etag` to update or release the mutex.
  
* Mutex can be updated even after it technically expired, but has not been re-acquired by anyone else as long as `etag` matches the old value

* When mutex is release its score is set to -1 thus eliminating it from search.

## Examples

Start redis:

```bash
docker run --name my-redis -p 6379:6379 -d redis
```

Python use case:

```python
import oci
from uuid import uuid4
from time import sleep
from threading import Thread
from mutex import NoSQLTasks
  
config = oci.config.from_file("~/.oci/config")
nosql = oci.nosql.NosqlClient(config=config)
nosql_table = nosql.get_table(table_name_or_id='ocid1.nosqltable.oc1.eu-frankfurt-1.amaaaa....')

owner = str(uuid4())
tasks = NoSQLTasks(client=nosql, table_name_or_id=nosql_table.data.id, owner=owner)

mutex_name = str(uuid4())

def func1():
  # Let thread t3 enter blocking acquire()
  sleep(10)
  # Create named mutex mutex_name
  _ok = tasks.create(mutex_name)

def func2():
  # Let thread t3 acquire mutex_name mutex
  sleep(20)
  # blocking (120s) call to acquire any available mutex
  # mutex_name becomes available timeout_seconds (60s) after last update/acquire
  _mutex = tasks.acquire(timeout=60)
  sleep(10)
  _mutex = tasks.release(_mutex)

def func3():
  # blocking (30s) call to acquire named mutex mutex_name
  _mutex = tasks.acquire(timeout=30, lock_id=mutex_name)
  sleep(10)  
  _mutex = tasks.update(_mutex)
  # mutex mutex_name expires after timeout_seconds (60s) since last update/acquire

t1=Thread(target=func1)
t2=Thread(target=func2)
t3=Thread(target=func3)
t1.start()
t2.start()
t3.start()
t1.join()
t2.join()
t3.join()
```
