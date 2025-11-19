# 管理对PostgreSQL数据库的连接
import psycopg2
import psycopg2.extras
import pandas as pd
import os
from clients.mcp_client_interface import IMCPClient
import re

class PostgreSQLMCPHandler(IMCPClient):
    def __init__(self):
        super().__init__()
        self.postgresql_connect = None

    def connect(self):
        if not self.postgresql_connect:
            # conn = psycopg2.connect("dbname=test user=postgres password=secret")
            # conn = psycopg2.connect(database="test", user="postgres", password="secret")
            dsn = os.getenv("DSN")
            print("没有检测到已有链接，尝试根据配置DSN连接数据库:", dsn)
            try:
                temp_postgresql_connect = psycopg2.connect(dsn)
                self.postgresql_connect = temp_postgresql_connect
                print("成功连接到数据库")
            except Exception as e:
                print(f"根据DSN连接数据库失败: {str(e)}")

    def target_db(self, dbname):
        """
        切换到指定的数据库
        
        Args:
            dbname: 目标数据库名称
        """
        if self.postgresql_connect:
            self.postgresql_connect.close()
            self.postgresql_connect = None
            print("已关闭现有连接，准备连接到新的数据库")
        
        dsn = os.getenv("DSN")
        if not dsn:
            raise ValueError("环境变量 DSN 未设置，无法建立新连接")
        
        try:
            # 修改 DSN 中的数据库名称为目标数据库
            new_dsn = re.sub(r'dbname=\w+', f'dbname={dbname}', dsn)
            self.postgresql_connect = psycopg2.connect(new_dsn)
            print(f"成功连接到新数据库: {dbname}")
        except Exception as e:
            print(f"连接新数据库失败: {str(e)}")
            raise
    
    def close(self):
        if self.postgresql_connect:
            self.postgresql_connect.close()
            self.postgresql_connect = None
            print("数据库连接已关闭")
    
    def isConnected(self) -> bool:
        return self.postgresql_connect is not None

    def execute(self, sql: str) -> str:
        if not self.postgresql_connect:
            raise ConnectionError("数据库连接未初始化，请先调用connect()方法")
        try:
            with self.postgresql_connect.cursor() as cursor:
                cursor.execute(sql)
                self.postgresql_connect.commit()
                print("SQL执行成功")
                return "SQL执行成功"
        except Exception as e:
            print(f"SQL执行失败: {str(e)}")
            self.postgresql_connect.rollback()
            return f"SQL执行失败: {str(e)}"
    
    def query(self, sql: str) -> pd.DataFrame:
        """
        执行SQL查询并返回pandas DataFrame结果
        
        Args:
            sql: 要执行的SQL查询语句
            
        Returns:
            pandas.DataFrame: 查询结果
            
        Raises:
            ConnectionError: 当数据库连接未初始化时
            Exception: 当查询执行失败时
        """
        if not self.postgresql_connect:
            raise ConnectionError("数据库连接未初始化，请先调用connect()方法")
        try:
            # 打印即将连接的数据库与执行的SQL，方便排查
            print(f"连接数据库: {self.postgresql_connect.get_dsn_parameters().get('dbname', 'unknown')}")
            print(f"执行查询: {sql}")
            # 使用pandas的read_sql_query方法直接从数据库读取数据到DataFrame
            # 注意：read_sql_query只能处理返回结果集的SQL（如SELECT），对于DDL/DML（如COMMENT）会返回None，导致'NoneType' object is not iterable
            df = pd.read_sql_query(sql, self.postgresql_connect)
            # 如果df为None（例如执行COMMENT语句），返回空DataFrame避免后续迭代报错
            if df is None:
                print(f"查询成功，但返回df为空")
            else:
                print(f"查询成功，返回{len(df)}行数据")

            return df
        except Exception as e:
            print(f"查询执行失败: {str(e)}")
            raise


