"""
PostgreSQL 查询数据处理器 - 负责执行SQL查询
[暂时弃用]先使用LangChain自带的数据库处理工具
"""

from typing import Any
from ...mcp_handler_interface import IMCPHandler

class PostgreSQLQueryHandler(IMCPHandler):
    def __init__(self, client):
        self.client = client
        
    def register_tools(self, mcp:Any):
        """
        注册PostgreSQL查询工具
        :param mcp: MCP实例，用于注册工具
        """

        # @mcp.tool()
        def execute_sql(sql: str):
            """
            执行PostgreSQL SQL语句
            :param sql: 要执行的SQL语句
            :return: 执行结果（成功/失败）
            """
            try:
                result = self.client.execute(sql)
                return result
            except Exception as e:
                return f"SQL执行失败: {str(e)}"

        # @mcp.tool()
        def query_postgresql(sql: str):
            """
            执行PostgreSQL查询
            :param sql: 要执行的SQL查询语句
            :return: 查询结果（如果有）
            """
            try:
                df = self.client.query(sql)
                if df is None:
                    return "查询执行成功，但返回空结果集"

                return df.to_dict(orient='records')
            except Exception as e:
                return f"查询执行失败: {str(e)}"

        # @mcp.tool("check_connect_postgresql")
        def check_connect():
            """
            检查与PostgreSQL数据库的连接状态
            :return: 连接状态（已连接/未连接）
            """
            if self.client.isConnected():
                return "已连接"
            else:
                return "未连接"

        # @mcp.tool("connect_postgresql")
        def connect():
            """
            连接到PostgreSQL数据库
            :return: 连接状态（已连接/未连接）
            """
            try:
                self.client.connect()
                return "已连接"
            except Exception as e:
                return f"连接失败: {str(e)}"

        # @mcp.tool()
        def connect_to_target_db(dbname: str):
            """
            连接到目标PostgreSQL数据库
            :param dbname: 目标数据库名称
            :return: 连接状态（已连接/未连接）
            """
            try:
                self.client.target_db(dbname)
                return f"已连接到数据库: {dbname}"
            except Exception as e:
                return f"连接数据库失败: {str(e)}"

        # @mcp.tool("close_postgresql")
        def close():
            """
            关闭与PostgreSQL数据库的连接
            :return: 连接状态（已连接/未连接）
            """
            try:
                self.client.close()
                return "已关闭"
            except Exception as e:
                return f"关闭连接失败: {str(e)}"

    def register_resources(self, mcp:Any):
        pass

    def register_prompts(self, mcp:Any):
        pass