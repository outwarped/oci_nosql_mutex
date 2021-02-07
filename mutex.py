import oci
import logging
from datetime import datetime
from typing import Optional, Tuple, Collection, Collection

logger = logging.getLogger(__name__)


class NoSQLTasks(object):
    def __init__(self, client:oci.nosql.NosqlClient, table_name_or_id:str, owner:str=None, timeout_seconds:int=60):
        super(NoSQLTasks, self).__init__()
        
        self._client = client
        self._table = self._client.get_table(
            table_name_or_id=table_name_or_id
        ).data
        self._timeout = timeout_seconds * 1000000
        self._owner = owner
    
    def create(self, lock_id:str, owner:str=None, body:dict=None) -> bool:
        res = self._client.update_row(
            table_name_or_id=self._table.id,
            update_row_details=oci.nosql.models.UpdateRowDetails(
                value={
                    "key": lock_id,
                    "score": 0,
                    "owner": owner,
                    "body": body,
                },
                option="IF_ABSENT",
                is_get_return_row=True,
                is_exact_match=True,
            ),
        )
        if not res.data.version is None:
            return True
        
        return False
    
    def _stale_generator(self, include_unattended=True, include_expired=True, refresh=60, lock_id_mask:Optional[str]=None) -> Collection[str]:
        if not (include_unattended or include_expired):
            return
        
        while True:
            iteration_time = datetime.now().timestamp()
            xs = self._get_stale(include_unattended=include_unattended, include_expired=include_expired, lock_id_mask=lock_id_mask)
            for x in xs:
                yield x
                if datetime.now().timestamp() > iteration_time + refresh:
                    break
        
    def _get_stale(self, include_unattended=True, include_expired=True, lock_id_mask:Optional[str]=None) -> Collection[str]:
        if not (include_unattended or include_expired):
            return []
        
        lock_id_mask = " AND key = \"{}\"".format(lock_id_mask) if lock_id_mask is None else ""
        
        start = 0 if include_unattended else 1
        end = 1 if not include_expired else int(datetime.now().timestamp() * 1000000) - self._timeout
    
        res = self._client.query(
            query_details=oci.nosql.models.QueryDetails(
                compartment_id=self._table.compartment_id,
                statement="SELECT key FROM {name} WHERE score >= {start} AND score < {end}{lock_id_mask}".format(lock_id_mask=lock_id_mask, name=self._table.name, start=start, end=end),
                is_prepared=False,
                consistency="EVENTUAL",
            ),
        )
        return list(map(lambda x: x["key"], res.data.items))
    
    def _instant_acquire_one(self, lock_id, etag:Optional[str]=None, score:Optional[int]=None, owner:str=None, body:dict=None) -> Optional[Tuple[str, str]]:
        value={"key": lock_id}
        if not owner is None:
            value.update({"owner": owner})
        if not body is None:
            value.update({"body": body})
        if not score is None:
            value.update({"score": score})
        else:
            now = datetime.now().timestamp() * 1000000
            value.update({"score": now})
            
        if etag is None:
            res = self._client.get_row(
                table_name_or_id=self._table.id,
                key=["key:{}".format(lock_id)],
            )
            if res.data.value is None:
                return None
            etag = res.headers["etag"]
            score = res.data.value["score"]
            body = res.data.value["body"]
            if score > int(datetime.now().timestamp() * 1000000) - self._timeout:
                return None
        
        res = self._client.update_row(
            table_name_or_id=self._table.id,
            update_row_details=oci.nosql.models.UpdateRowDetails(
                value=value,
                option="IF_PRESENT",
                is_get_return_row=True,
            ),
            if_match=etag,
        )
        if not res.data.version is None:
            return (lock_id, res.data.version, body)
        if not etag is None and res.data.existing_version == etag:
            return (lock_id, res.data.existing_version, res.data.existing_value["body"])
        
        return None

    def acquire(self, timeout:int=0, lock_id:Optional[str]=None, owner:str=None, body:dict=None) -> Optional[Tuple[str, str]]:
        owner = self._owner if owner is None else owner
        start = datetime.now().timestamp()
        lock_id_mask = "*" if lock_id is None else lock_id
        generator = self._stale_generator(include_unattended=True, include_expired=True, refresh=60, lock_id_mask=lock_id_mask)
        while timeout == 0 or start + timeout > datetime.now().timestamp():
            lock = next(generator)
            lock_result = self._instant_acquire_one(lock_id=lock, owner=owner, body=body)
            if not lock_result is None:
                return lock_result
        return None
    
    def update(self, lock:Tuple[str, str], owner:str=None, body:dict=None) -> Optional[Tuple[str, str]]:
        owner = self._owner if owner is None else owner
        return self._instant_acquire_one(lock_id=lock[0], etag=lock[1], owner=owner, body=body)
    
    def release(self, lock:Tuple[str, float], owner:str=None, body:dict=None) -> Optional[Tuple[str, float]]: 
        owner = self._owner if owner is None else owner
        return self._instant_acquire_one(lock_id=lock[0], etag=lock[1], score=1, owner=owner, body=body)
    
    def delete(self, lock:Tuple[str, float], owner:str=None, body:dict=None) -> bool:
        owner = self._owner if owner is None else owner
        return self._instant_acquire_one(lock_id=lock[0], etag=lock[1], score=-1, owner=owner, body=body)
