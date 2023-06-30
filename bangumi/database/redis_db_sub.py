from hashlib import md5
import logging
import os
import re
from time import time
from typing import List, Union
import sqlite3

from contextlib import closing
from bangumi.consts.env import Env
from bangumi.entitiy import WaitDownloadItem
from bangumi.util import from_dict_to_dataclass

logger = logging.getLogger(__name__)

#不想使用redis，整一个sqlite实现替换
class RedisDBSub(object):
    def __init__(self) -> None:
        self.client: sqlite3.Connection = None

    def connect(self) -> None:
        if self.client is not None:
            logger.debug("SQLiteDB already connected")
            return
        logger.info("Connecting to SQLite3...")
        
        #TODO 路径配置
        self.client = sqlite3.connect('BangumiDB.db')
        logger.info(f"Connected to SQLite3")
        #建表
        with closing(self.client.cursor()) as cursor:
            #信息列表
            cursor.execute('''CREATE TABLE IF NOT EXISTS pool_list 
                                (hash   TEXT     PRIMARY KEY NOT NULL,
                                 name   TEXT                NOT NULL,
                                 url    TEXT                NOT NULL,
                                 pub_at INTEGER             NOT NULL,
                                 length INTEGER             NOT NULL);''')
            
            cursor.execute('''CREATE TABLE IF NOT EXISTS queue 
                                (_id    INTEGER     PRIMARY KEY AUTOINCREMENT,
                                 hash   TEXT                NOT NULL,
                                 time   TIMESTAMP           NOT NULL DEFAULT CURRENT_TIMESTAMP);''')
            
            #放一些kv？
            cursor.execute('''CREATE TABLE IF NOT EXISTS mark 
                                (key    TEXT    PRIMARY KEY NOT NULL,
                                 value  TEXT                NOT NULL);''')   
            #下载状态表
            cursor.execute('''CREATE TABLE IF NOT EXISTS file 
                                (hash       TEXT    PRIMARY KEY NOT NULL,
                                 downloaded BOOLEAN             NOT NULL);''') 
            #做种表
            cursor.execute('''CREATE TABLE IF NOT EXISTS seeding 
                                (hash       TEXT    PRIMARY KEY NOT NULL,
                                 seeding    BOOLEAN             NOT NULL);''')
        self.client.commit()
            
    def get(self, hash_: str) -> WaitDownloadItem:
        #查询item
        with closing(self.client.cursor()) as cursor:
            cursor.execute(f"SELECT * FROM pool_list WHERE hash='{hash_}'")
            result = cursor.fetchone()
            if result == None:
                return None
            else:
                return WaitDownloadItem(
                                name=result[1],
                                url=result[2],
                                pub_at=result[3],
                                content_length=result[4])

    def remove(self, hash_: str) -> None:
        #删除item
        if not self.client: 
            return
        with closing(self.client.cursor()) as cursor:
            cursor.execute(f"DELETE FROM pool_list WHERE hash='{hash_}'")
            cursor.execute(f"DELETE FROM queue WHERE hash='{hash_}'")
        self.client.commit()

    def update_last_checked_time(self):
        #更新最后检查的时间
        if not self.client: 
            return
        with closing(self.client.cursor()) as cursor:
            cursor.execute("REPLACE INTO mark (key, value) values ('last_checked_time', '"+ str(int(time())) +"')")
        self.client.commit()

    def get_last_checked_time(self) -> int:
        #获取最后检查的时间
        if not self.client:
            return 0
        with closing(self.client.cursor()) as cursor:
            cursor.execute("SELECT value FROM mark WHERE key='last_checked_time'")
            result = cursor.fetchone()
            if result == None:
                return 0
            else:
                return int(result[0])

    def add_to_torrent_queue(
        self, items: Union[WaitDownloadItem, List[WaitDownloadItem]]
    ) -> None:
        #添加到队列
        if isinstance(items, WaitDownloadItem):
            items = [items]
        for item in items:
            with closing(self.client.cursor()) as cursor:
                cursor.execute(f"INSERT INTO queue (hash) values ('{item.hash}')")
                cursor.execute(f"REPLACE INTO pool_list (name, url, hash, pub_at, length) values ('{item.name}', '{item.url}','{item.hash}',{item.pub_at},{item.content_length})")
        self.client.commit()

    def pop_torrent_to_download(self) -> WaitDownloadItem:
        try:
            with closing(self.client.cursor()) as cursor:
                cursor.execute(f"SELECT * FROM queue ORDER BY time LIMIT 1")
                result = cursor.fetchone()
                if result == None:
                    return None
                else:
                    hash_ = result[0]
                    cursor.execute(f"DELETE FROM queue WHERE hash='{hash_}'")
                    return self.get(hash_)
        finally:
            self.client.commit()
            

    def get_pending(self) -> List[WaitDownloadItem]:
        """
        返回还未添加进下载队列
        """
        with closing(self.client.cursor()) as cursor:
            cursor.execute(f"SELECT * FROM queue ORDER BY time LIMIT 50")
            hash_arr = cursor.fetchall()
            if hash_arr == None:
                return None
            else:
                print(hash_arr)
                return [self.get(hash_[1]) for hash_ in hash_arr]
        

    def get_key_from_formatted_name(self, name: str) -> str:
        ret = re.match(r"(.*) (S\d+E\d+)", name.strip())
        if not ret:
            return
        name, ext = ret.groups()
        return name.strip().replace(" ", "_") + ":" + ext

    def is_downloaded(self, formatted_name: str) -> bool:
        key = self.get_key_from_formatted_name(formatted_name)
        if not key:
            return False
        with closing(self.client.cursor()) as cursor:
            cursor.execute(f"SELECT downloaded FROM file WHERE hash='{key}'")
            result = cursor.fetchone()
            if result == None:
                return False
            else:
                return bool(result[0])

    def set_downloaded(self, formatted_name: str):
        key = self.get_key_from_formatted_name(formatted_name)
        if not key:
            return
        with closing(self.client.cursor()) as cursor:
            cursor.execute(f"REPLACE INTO file (hash, downloaded) values ('{key}', True)")
        self.client.commit()

    def is_seeding(self, _hash: str) -> bool:
        with closing(self.client.cursor()) as cursor:
            cursor.execute(f"SELECT seeding FROM seeding WHERE hash='{_hash}'")
            result = cursor.fetchone()
            if result == None:
                return False
            else:
                return bool(result[0])

    def set_seeding(self, _hash: str) -> None:
        with closing(self.client.cursor()) as cursor:
            cursor.execute(f"REPLACE INTO seeding (hash, seeding) values ('{_hash}', True)")
        self.client.commit()

    def init(self) -> bool:
        if not self.client:
            return False
        with closing(self.client.cursor()) as cursor:
            cursor.execute("SELECT value FROM mark WHERE key='initd'")
            result = cursor.fetchone()
            if result == None or not bool(result[0]):
                return False
            cursor.execute("REPLACE INTO mark (key, value) values ('initd', 'True')")
        self.client.commit()
        return True
    

    def close_db(self) ->None:
        if self.client:
            self.client.close()
