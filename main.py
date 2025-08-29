
import mysql.connector
from manticoresearch import Configuration, ApiClient
from manticoresearch.api import IndexApi, UtilsApi
from typing import Dict, List, Any
from decimal import Decimal
import logging
import time
from datetime import datetime
import json

class DatabaseSyncTool:
    def __init__(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
        self.mysql_config = self._get_mysql_config()
        self.manticore_config = self._get_manticore_config()
        self.client = ApiClient(self.manticore_config)
        self.index_api = IndexApi(self.client)
        self.utils_api = UtilsApi(self.client)
        self.batch_size = 1000

    def _get_mysql_config(self) -> Dict[str, Any]:
        return {
            'host': '192.168.2.6',
            'user': 'root',
            'password': 'bzdmmynj',
            'database': 'bkshop',
            'ssl_disabled': True,
            'connection_timeout': 600
        }

    def _get_manticore_config(self) -> Configuration:
        config = Configuration()
        config.host = "http://192.168.2.6:9308"
        config.verify_ssl = False
        return config

    def get_all_tables(self) -> List[str]:
        retry_count = 0
        max_retries = 3
        while retry_count < max_retries:
            try:
                with mysql.connector.connect(**self.mysql_config) as conn:
                    with conn.cursor() as cursor:
                        cursor.execute("SHOW TABLES")
                        tables = [table[0] for table in cursor.fetchall()]
                        self.logger.info(f"发现 {len(tables)} 个需要同步的表")
                        return tables
            except mysql.connector.Error as err:
                retry_count += 1
                self.logger.error(f"获取表列表失败(尝试 {retry_count}/{max_retries}): {err}")
                time.sleep(2)
        return []

    def sync_table(self, table_name: str) -> bool:
        try:
            self.logger.info(f"开始同步表: {table_name}")
            
            # 获取表结构
            schema = self._get_table_schema(table_name)
            if not schema:
                return False
                
            # 创建Manticore索引
            if not self._create_manticore_index(table_name, schema):
                return False
                
            # 同步数据
            return self._sync_data(table_name)
            
        except Exception as e:
            self.logger.error(f"同步表 {table_name} 失败: {str(e)}")
            return False

    def _get_table_schema(self, table_name: str) -> Dict[str, Any]:
        with mysql.connector.connect(**self.mysql_config) as conn:
            with conn.cursor(dictionary=True) as cursor:
                cursor.execute(f"DESCRIBE {table_name}")
                return {row['Field']: row for row in cursor.fetchall()}

    def _create_manticore_index(self, table_name: str, schema: Dict[str, Any]) -> bool:
        try:
            fields = []
            for field, config in schema.items():
                field_type = self._map_mysql_to_manticore_type(config['Type'])
                RESERVED_WORDS = {'order', 'rank', 'match', 'select', 'group', 'by'}
                if field.lower() in RESERVED_WORDS:
                    field_def = f"`new_{field}` {field_type}"
                else:
                    field_def = f"`{field}` {field_type}"

                fields.append(field_def)
                
            self.utils_api.sql(f"DROP TABLE IF EXISTS {table_name}")
            create_sql = f"CREATE TABLE {table_name} ({', '.join(fields)})"

            self.utils_api.sql(create_sql)
            return True
        except Exception as e:
            self.logger.error(create_sql)
            self.logger.error(f"创建索引失败: {str(e)}")
            return False

    def _map_mysql_to_manticore_type(self, mysql_type: str) -> str:
        if 'int' in mysql_type.lower():
            return 'integer'
        elif 'float' in mysql_type.lower() or 'double' in mysql_type.lower() or 'decimal' in mysql_type.lower():
            return 'float'
        elif 'char' in mysql_type.lower() or 'text' in mysql_type.lower():
            return 'text'
        elif 'date' in mysql_type.lower() or 'time' in mysql_type.lower():
            return 'timestamp'
        else:
            return 'text'

    def _sync_data(self, table_name: str) -> bool:
        offset = 0
        while True:
            try:
                with mysql.connector.connect(**self.mysql_config) as conn:
                    with conn.cursor(dictionary=True) as cursor:
                        query = f"SELECT * FROM `{table_name}` LIMIT %s OFFSET %s"
                        cursor.execute(query, (self.batch_size, offset))
                        rows = cursor.fetchall()
                        if not rows:
                            break

                        # 构建符合Manticore Bulk API的请求体
                        bulk_actions = []
                        for row in rows:
                            if 'id' not in row:
                                doc = self.prepare_document(row)
                                bulk_actions.append(json.dumps({
                                    "insert": {
                                        "index": table_name,
                                        "doc": doc
                                    }
                                }))
                                # raise ValueError(f"文档缺少ID字段: {row}")
                            else:
                                doc = self.prepare_document(row)
                                # 将 insert 操作改为 replace 或 update 可自动处理冲突
                                bulk_actions.append(json.dumps({
                                    "replace": {
                                        "index": table_name,
                                        "id": row['id'],
                                        "doc": doc
                                    }
                                }))
                            # 每个操作必须用换行符分隔
                            payload = "\n".join(bulk_actions)
                            self.index_api.bulk(payload)
                            
                            offset += len(rows)
                            # self.logger.info(f"已同步 {table_name} {offset} 条记录")
                        
            except mysql.connector.Error as db_err:
                self.logger.error(f"数据库错误: {db_err}")
                return False
            except Exception as e:
                self.logger.error(f"同步失败: {str(e)}")
                return False
                
        self.logger.info(f"同步完成，总计 {offset} 条记录")
        return True

    def prepare_document(self, row: Dict) -> Dict:
        """转换数据类型确保兼容Manticore"""
        doc = {}
        for ks, v in row.items():
            if ks == 'id':  # 跳过id列
                continue
            if v is None:
                continue
            """转义关键词"""
            RESERVED_WORDS = {'order', 'rank', 'match', 'select', 'group', 'by'}
            if ks.lower() in RESERVED_WORDS:
                ks = f"new_{ks}"
                
            if isinstance(v, (bytes, bytearray)):
                doc[ks] = v.decode('utf-8')
            elif isinstance(v, (dict, list)):
                doc[ks] = json.dumps(v)
            elif isinstance(v, (Decimal)):
                doc[ks] = str(v)
                return str(v)  # 保持精度
            elif isinstance(v, datetime):
                return v.isoformat()
            else:
                doc[ks] = v
        return doc

if __name__ == "__main__":
    sync_tool = DatabaseSyncTool()
    tables = sync_tool.get_all_tables()
    for table in tables:
        sync_tool.sync_table(table)
