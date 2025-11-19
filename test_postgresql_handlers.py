from clients.postgresql.postgresql_mcp_client import PostgreSQLMCPClient

# 创建客户端实例
client = PostgreSQLMCPClient()
print("客户端创建成功")
print(f"是否包含handlers属性: {hasattr(client, 'handlers')}")
print(f"handlers类型: {type(client.handlers)}")
print(f"handlers内容: {client.handlers}")
print(f"handlers数量: {len(client.handlers)}")

# 尝试访问具体的handler
if 'PostgreSQLQueryHandler' in client.handlers:
    print("PostgreSQLQueryHandler存在")
    handler = client.handlers['PostgreSQLQueryHandler']
    print(f"Handler客户端类型: {type(handler.client)}")
else:
    print("PostgreSQLQueryHandler不存在")