# mongo_base.py
# -- coding: utf-8 --

import sys
import threading
import urllib.parse
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from pymongo import ASCENDING, DESCENDING, MongoClient, UpdateOne
from pymongo.errors import PyMongoError

from common.common_utils import get_config, logger


class MongoBase:
    _instance = None
    _client = None
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        """单例模式：确保全局只维护一个数据库连接池"""
        if not cls._instance:
            with cls._lock:
                if not cls._instance:
                    cls._instance = super(MongoBase, cls).__new__(cls)
        return cls._instance

    def __init__(
        self,
        host="localhost",
        port=27017,
        username=None,
        password=None,
        db_name="admin",
        auth_source="admin",
        max_pool_size=100,
    ):
        if not self._client:
            with self._lock:
                if not self._client:
                    if username and password:
                        username = urllib.parse.quote_plus(username)
                        password = urllib.parse.quote_plus(password)
                        uri = f"mongodb://{username}:{password}@{host}:{port}/{auth_source}"
                    else:
                        uri = f"mongodb://{host}:{port}/"

                    # connect=False 防止多进程下的死锁问题
                    self._client = MongoClient(uri, maxPoolSize=max_pool_size, connect=False)
                    self.db = self._client[db_name]
                    logger.info(f"[数据库/连接] MongoDB 连接池就绪 | 节点: [{host}:{port}] | 数据库: [{db_name}]")

    def get_collection(self, collection_name):
        return self.db[collection_name]

    def find_many(self, collection_name, query=None, projection=None, limit=0, sort=None):
        """通用查询"""
        if query is None:
            query = {}
        col = self.get_collection(collection_name)
        cursor = col.find(query, projection)
        if sort:
            cursor = cursor.sort(sort)
        if limit and limit > 0:
            cursor = cursor.limit(limit)
        return list(cursor)

    def delete_many(self, collection_name, query: dict) -> int:
        """通用批量删除"""
        if not query:
            return 0
        col = self.get_collection(collection_name)
        res = col.delete_many(query)
        return int(res.deleted_count)

    def bulk_upsert(self, collection_name, data_list, unique_key_field):
        """
        核心方法：高性能批量 Upsert。
        支持单字段或多字段组成的联合唯一键。
        """
        if not data_list:
            return

        collection = self.get_collection(collection_name)
        operations = []
        unique_keys = unique_key_field if isinstance(unique_key_field, list) else [unique_key_field]

        for item in data_list:
            update_data = item.copy()
            query = {}
            missing_key = False

            for key in unique_keys:
                val = update_data.pop(key, None)
                if val is None:
                    missing_key = True
                    break
                query[key] = val

            if missing_key:
                logger.warning(
                    f"[数据库/写入] 数据项缺少唯一键，已跳过该条记录 | 集合: [{collection_name}] | 必填键: [{unique_keys}]"
                )
                continue

            operations.append(UpdateOne(query, {"$set": update_data}, upsert=True))

        if operations:
            try:
                collection.bulk_write(operations, ordered=False)
            except Exception as e:
                logger.error(
                    f"❌ [数据库/写入] 批量 Upsert 写入失败 | 集合: [{collection_name}] | 待写条数: [{len(operations)}] "
                    f"| 原因: [{e}] | 排查方向: [检查 MongoDB 服务状态、网络连通性及唯一索引约束]"
                )
                raise

    def create_index(self, collection_name, keys, unique=False):
        """创建索引，已存在则自动忽略，不会报错"""
        try:
            col = self.get_collection(collection_name)
            col.create_index(keys, unique=unique, background=True)
        except PyMongoError as e:
            logger.warning(f"[数据库/索引] 索引校验告警 | 集合: [{collection_name}] | 索引键: [{keys}] | 原因: [{e}]")


def gen_db_object():
    """生成数据库连接实例"""
    HOST = get_config("local_mongo_host")
    PORT = get_config("local_mongo_port")
    USERNAME = get_config("local_mongo_user")
    PASSWORD = get_config("local_mongo_password")
    DB_NAME = get_config("local_mongo_db_name")

    return MongoBase(
        host=HOST,
        port=PORT,
        username=USERNAME,
        password=PASSWORD,
        db_name=DB_NAME,
        max_pool_size=50,
    )
