
import mysql.connector
from manticoresearch import Configuration, ApiClient
from manticoresearch.api import IndexApi, UtilsApi
from typing import Dict, List, Any, Union
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
        self.pri = {}
        self.table_field_type = {}
        self.keywords = {'order', 'rank', 'match', 'select', 'desc', 'group', 'by'}

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
                cursor.execute(f"DESCRIBE `{table_name}`")
                return {row['Field']: row for row in cursor.fetchall()}

    def _create_manticore_index(self, table_name: str, schema: Dict[str, Any]) -> bool:
        try:
            fields = []
            for field, config in schema.items():
                if config['Key'] == 'PRI' and 'int' in config['Type'].lower():
                    self.pri[table_name] = field

                # 表结构转换 mysql 
                field_type = self._map_mysql_to_manticore_type(config['Type'])
                # 表结构字段类型
                self.table_field_type[f"{table_name}-{field}"] = field_type

                if field.lower() in self.keywords:
                    field_def = f"`new_{field}` {field_type}"
                elif field == 'id':
                    field_def = f"`table_{field}` {field_type}"
                else:
                    field_def = f"`{field}` {field_type}"
                    
                fields.append(field_def)
                
            self.utils_api.sql(f"DROP TABLE IF EXISTS `{table_name}`")
            create_sql = f"CREATE TABLE `{table_name}` ({', '.join(fields)})"
            self.utils_api.sql(create_sql)

            # self.logger.info(create_sql)
            return True
        except Exception as e:
            self.logger.error(create_sql)
            self.logger.error(f"创建索引失败: {str(e)}")
            return False

    def _map_mysql_to_manticore_type(self, mysql_type: str) -> str:
        # 'string': '',
        # 'text': '',
        # 'integer': 0,
        # 'float': 0.0,
        # 'bool': False,
        # 'json': [],
        # 'timestamp': 0
        # self.logger.info(mysql_type)
        # self.logger.info('char' in mysql_type.lower())

        if 'int' in mysql_type.lower():
            return 'integer'
        elif 'float' in mysql_type.lower() or 'double' in mysql_type.lower():
            return 'float'
        elif 'char' in mysql_type.lower() or 'decimal' in mysql_type.lower():
            return 'string'
        elif 'text' in mysql_type.lower():
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
                        print(1111111111111)
                        query = f"SELECT * FROM `{table_name}` LIMIT %s OFFSET %s"
                        cursor.execute(query, (self.batch_size, offset))
                        rows = cursor.fetchall()
                        if not rows:
                            break
                        
                        # 构建符合Manticore Bulk API的请求体
                        bulk_actions = []
                        for row in rows:
                            doc = self.prepare_document(row, table_name)
                            # if 'id' not in row:
                            # 方法1：直接判断键是否存在
                            if table_name not in self.pri or self.pri[table_name] not in row:
                                bulk_actions.append(json.dumps({
                                    "replace": {
                                        "index": table_name,
                                        "doc": doc
                                    }
                                }))
                                # raise ValueError(f"文档缺少ID字段: {row}")
                            else:
                                # 将 insert 操作改为 replace 或 update 可自动处理冲突
                                bulk_actions.append(json.dumps({
                                    "replace": {
                                        "index": table_name,
                                        "id": row[self.pri[table_name]],
                                        "doc": doc
                                    }
                                }))
                            
                        # 
                        # 每个操作必须用换行符分隔
                        payload = "\n".join(bulk_actions)
                        # error
                        # self.logger.error(1111111111111111111)
                        # self.logger.error(payload)
                        # self.logger.error(2222222222222222222)
                        # bulk
                        respond = self.index_api.bulk(payload)
                        self.logger.error(respond)

                        offset += len(rows)
                        # self.logger.info(f"已同步 {table_name} {offset} 条记录")
            except mysql.connector.Error as db_err:
                self.logger.error(f"数据库错误: {db_err}")
                return False
            except Exception as e:
                self.logger.error(payload)
                self.logger.error(f"同步失败: {str(e)}")
                return False
                
        self.logger.info(f"同步完成，总计 {offset} 条记录")
        return True

    def handle_null(self, field_type: str) -> Union[str, int, float, list]:
        """根据字段类型返回对应的NULL替代值"""
        type_handlers = {
            'string': '',
            'text': '',
            'integer': 0,
            'float': 0.0,
            'bool': False,
            'json': [],
            'timestamp': 0
        }
        return type_handlers.get(field_type.lower(), '')

    def prepare_document(self, row: Dict, table_name: str) -> Dict:
        """转换数据类型确保兼容Manticore"""
        doc = {}
        for k, v in row.items():
            """转义关键词"""
            if k.lower() in self.keywords:
                k = f"new_{k}"
            if k == 'id':  # 跳过id列
                k = f"table_{k}"
                # continue

            # if v is None:
            #     continue
            """Type conversion dispatcher"""
            if isinstance(v, (bytes, bytearray)):
                doc[k] = v.decode('utf-8', errors='replace')
            elif isinstance(v, (dict, list)):
                doc[k] = json.dumps(v, ensure_ascii=False)
            elif isinstance(v, Decimal):
                doc[k] = float(v)
            elif isinstance(v, datetime):
                doc[k] = int(v.timestamp())
            else:
                doc[k] = v

            if v is None:
                doc[k] = self.handle_null(self.table_field_type[f"{table_name}-{k}"])
            #     continue
        # self.logger.error(doc)
        return doc

if __name__ == "__main__":
    sync_tool = DatabaseSyncTool()
    tables = sync_tool.get_all_tables()
    for table in tables:
        sync_tool.sync_table(table)
        # break