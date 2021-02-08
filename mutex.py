import oci
import logging
from datetime import datetime
from random import shuffle
from typing import Optional, Tuple, Collection, Generator

logger = logging.getLogger(__name__)


class NoSQLTasks(object):
    def __init__(self, client:oci.nosql.NosqlClient, table_name_or_id:str, owner:str=None, timeout_seconds:int=60):
        """[Mutex control. Uses a NoSQL table to as concurrently-safe store.]

        Args:
            client (oci.nosql.NosqlClient): [OCI NoSQL Client]
            table_name_or_id (str): [Table OID or Name]
            owner (str, optional): [Sets Mutex owner field. To keep track of who updated the mutex last]. Defaults to None.
            timeout_seconds (int, optional): [Mutex timeout in seconds. After mutex times out it can be re-acquired by another owner]. Defaults to 60.
        """
        super(NoSQLTasks, self).__init__()
        
        self._client = client
        self._table = self._client.get_table(
            table_name_or_id=table_name_or_id
        ).data
        self._timeout = timeout_seconds * 1000000
        self._owner = owner
    
    def create(self, lock_id:str, owner:str=None, body:dict=None, auto_acquire=False) -> Optional[Tuple[str, str, dict]]:
        """[Create Mutex]

        Args:
            lock_id (str): [mutex name key]
            owner (str, optional): [if Mutex is acquire set new owner value, if provided]. Defaults to None.
            body (dict, optional): [if Mutex is acquire set new body value, if provided]. Defaults to None.
            auto_acquire (bool, optional): [if True Mutex can be re-acquired after it times out, if False Mutex can be re-acquired immediately]. Defaults to False
        Returns:
            Optional[Tuple[str, str, dict]]: [If created returns Mutex name (key) value, Mutex version (etag), Mutex body]
        """
        # score 0 means "unattended" indicating that Mutex has never been acquired
        score = 0 if not auto_acquire else int(datetime.now().timestamp() * 1000000)
        
        res = self._client.update_row(
            table_name_or_id=self._table.id,
            update_row_details=oci.nosql.models.UpdateRowDetails(
                value={
                    "key": lock_id,
                    "score": score,
                    "owner": owner,
                    "body": body,
                },
                option="IF_ABSENT",
                is_get_return_row=True,
                is_exact_match=True,
            ),
        )
        if not res.data.version is None:
            return lock_id, res.data.version, body
        
        return None
    
    def _stale_generator(self, include_unattended=True, include_expired=True, refresh=60, lock_id_mask:Optional[str]=None) -> Generator[str, None, None]:
        """[Iterates in an infinite loop through _get_stale() returned data. 
        Discards old collection and performs new _get_stale() search after _refresh_ seconds.
        Performs new _get_stale() call if returned data recods list has been exhausted]

        Args:
            include_unattended (bool, optional): [Include unattended]. Defaults to True.
            include_expired (bool, optional): [Include expired]. Defaults to True.
            refresh (int, optional): [Makes sure returned values are at most _refresh_ seconds old]. Defaults to 60.
            lock_id_mask (Optional[str], optional): [RegExp. Allow only specific keys. If None allows ALL]. Defaults to None.

        Yields:
            Generator[str, None, None]: [Found mutex keys]
        """
        # break if there is nothing to search
        if not (include_unattended or include_expired):
            return
        
        while True:
            iteration_time = datetime.now().timestamp()
            xs = self._get_stale(include_unattended=include_unattended, include_expired=include_expired, lock_id_mask=lock_id_mask)
            for x in xs:
                yield x
                # If older than refresh break from the loop to get new data
                if datetime.now().timestamp() > iteration_time + refresh:
                    break
        
    def _get_stale(self, include_unattended=True, include_expired=True, lock_id_mask:Optional[str]=None) -> Collection[str]:
        """[Lists all mutexes that has not been deleted or unattended or expired]

        Args:
            include_unattended (bool, optional): [Include unattended]. Defaults to True.
            include_expired (bool, optional): [Include expired]. Defaults to True.
            lock_id_mask (Optional[str], optional): [RegExp. Allow only specific keys. If None allows ALL]. Defaults to None.

        Returns:
            Collection[str]: [Found mutex keys]
        """
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
        # shuffle keys to reduce concurrent access load on first elements
        res = list(map(lambda x: x["key"], res.data.items))
        shuffle(res)
        return res
    
    def _instant_acquire_one(self, lock_id:str, etag:Optional[str]=None, score:Optional[int]=None, owner:str=None, body:dict=None) -> Optional[Tuple[str, str, dict]]:
        """[method acquires (locks) single record. If previous version is not provided it makes sure the record exists and expired]

        Args:
            lock_id ([str]): [mutex name key]
            etag (Optional[str], optional): [Previous version of the mutex, aka etag]. Defaults to None.
            score (Optional[int], optional): [if Mutex is acquire set new score value, in nanoseconds. Also Mutex validity is checked against this score]. Defaults to None.
            owner (str, optional): [if Mutex is acquire set new owner value, if provided]. Defaults to None.
            body (dict, optional): [if Mutex is acquire set new body value, if provided]. Defaults to None.

        Returns:
            Optional[Tuple[str, str, dict]]: [Mutex name (key) value, Mutex version (etag), Mutex body]
        """
        value={"key": lock_id}
        if not owner is None:
            value.update({"owner": owner})
        if not body is None:
            value.update({"body": body})
        if not score is None:
            value.update({"score": score})
        else:
            score = datetime.now().timestamp() * 1000000
            value.update({"score": score})
            
        # if etag is not provided get etag of the record. 
        # At the same time make sure record's score timed out.
        if etag is None:
            res = self._client.get_row(
                table_name_or_id=self._table.id,
                key=["key:{}".format(lock_id)],
            )
            if res.data.value is None:
                return None
            etag = res.headers["etag"]
            old_score = res.data.value["score"]
            body = res.data.value["body"]
            if old_score > score - self._timeout:
                return None
        
        # update score only if etag was not changed
        # thus ensure concurrent-safe acquire
        res = self._client.update_row(
            table_name_or_id=self._table.id,
            update_row_details=oci.nosql.models.UpdateRowDetails(
                value=value,
                option="IF_PRESENT",
                is_get_return_row=True,
                # is_exact_match=True,
            ),
            if_match=etag,
        )
        # record new version is returned
        if not res.data.version is None:
            return (lock_id, res.data.version, body)
        # somehow record exists and didnt get updated because it has the same score
        if not etag is None and res.data.existing_version == etag:
            return None
        
        return None

    def acquire(self, timeout:int=0, lock_id:Optional[str]=None, owner:str=None, body:dict=None) -> Optional[Tuple[str, str, dict]]:
        """[Acquire a mutex. Blocks call until a Mutex is available. Mutex can be re-acquired after it times out]

        Args:
            timeout (int, optional): [Block call until timeout, in secods. If 0 no timeout is appled]. Defaults to 0.
            lock_id (Optional[str], optional): [Mutex name (key) value. If not provided acquires first available mutex]. Defaults to None.
            owner (str, optional): [if Mutex is acquire set new owner value, if provided]. Defaults to None.
            body (dict, optional): [if Mutex is acquire set new body value, if provided]. Defaults to None.

        Returns:
            Optional[Tuple[str, str, dict]]: [if acquired returns Mutex name (key) value, Mutex version (etag), Mutex body]
        """
        owner = self._owner if owner is None else owner
        start = datetime.now().timestamp()
        generator = self._stale_generator(include_unattended=True, include_expired=True, refresh=60, lock_id_mask=lock_id)
        while timeout == 0 or start + timeout > datetime.now().timestamp():
            lock = next(generator)
            lock_result = self._instant_acquire_one(lock_id=lock, owner=owner, body=body)
            if not lock_result is None:
                return lock_result
        return None
    
    def update(self, lock:Tuple[str, str], owner:str=None, body:dict=None) -> Optional[Tuple[str, str, dict]]:
        """[Updates mutex's score, owner and body. Mutex can be re-acquired after it times out]

        Args:
            lock (Tuple[str, str]): [Mutex name (key) value. Mutex version (etag). Use tuple object that is returned by all functions which update Mutex state]
            owner (str, optional): [if Mutex is acquire set new owner value, if provided]. Defaults to None.
            body (dict, optional): [if Mutex is acquire set new body value, if provided]. Defaults to None.

        Returns:
            Optional[Tuple[str, str, dict]]: [if updated returns Mutex name (key) value, Mutex version (etag), Mutex body]
        """
        owner = self._owner if owner is None else owner
        return self._instant_acquire_one(lock_id=lock[0], etag=lock[1], owner=owner, body=body)
    
    def release(self, lock:Tuple[str, float], owner:str=None, body:dict=None) -> Optional[Tuple[str, str, dict]]:
        """[Sets mutex state to "expired" (score is set to 1). Mutex can be re-acquired immediately]

        Args:
            lock (Tuple[str, str]): [Mutex name (key) value. Mutex version (etag). Use tuple object that is returned by all functions which update Mutex state]
            owner (str, optional): [if Mutex is acquire set new owner value, if provided]. Defaults to None.
            body (dict, optional): [if Mutex is acquire set new body value, if provided]. Defaults to None.

        Returns:
            Optional[Tuple[str, str, dict]]: [if released returns Mutex name (key) value, Mutex version (etag), Mutex body]
        """
        owner = self._owner if owner is None else owner
        return self._instant_acquire_one(lock_id=lock[0], etag=lock[1], score=1, owner=owner, body=body)
    
    def delete(self, lock:Tuple[str, float], owner:str=None, body:dict=None) -> Optional[Tuple[str, str, dict]]:
        """[Removes mutex from future searches (score is set to -1). Mutex cannot be re-acquired]

        Args:
            lock (Tuple[str, str]): [Mutex name (key) value. Mutex version (etag). Use tuple object that is returned by all functions which update Mutex state]
            owner (str, optional): [if Mutex is acquire set new owner value, if provided]. Defaults to None.
            body (dict, optional): [if Mutex is acquire set new body value, if provided]. Defaults to None.

        Returns:
            Optional[Tuple[str, str, dict]]: [if deleted returns Mutex name (key) value, Mutex version (etag), Mutex body]
        """
        owner = self._owner if owner is None else owner
        return self._instant_acquire_one(lock_id=lock[0], etag=lock[1], score=-1, owner=owner, body=body)
