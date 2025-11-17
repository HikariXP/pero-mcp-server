import asyncio
import pytest_asyncio
import pytest
from fastmcp import FastMCP, Client

@pytest.mark.asyncio
async def test_tool_execution():
    # def inc(x):
    #     return x + 1

    # assert inc(3) == 5
    # 创建FastMCP服务器实例
    server = FastMCP("TestServer")
    
    # 注册calculate工具函数，用于测试
    @server.tool
    def calculate(x: int, y: int) -> int:
        return x + y
    
    # 使用异步上下文管理器创建Client连接到服务器
    async with Client(server) as client:
        # 调用calculate工具，传递参数x=5, y=3
        result = await client.call_tool("calculate", {"x": 5, "y": 3})
        
        # ====== 调试和检查对象类型的方法 ======
        # 1. 打印对象类型
        print(f"对象类型: {type(result)}")
        # 这行代码显示result对象的具体类型，帮助理解对象的结构
        
        # 2. 打印对象的完整表示
        print(f"对象完整表示: {repr(result)}")
        # repr()函数返回对象的正式字符串表示，包含更多内部结构信息
        
        # 3. 打印对象的字符串表示
        print(f"对象字符串表示: {str(result)}")
        # str()函数返回对象的用户友好字符串表示
        
        # ====== 检查对象的属性 ======
        # 可以使用dir()函数查看对象的所有属性和方法
        print(f"对象的所有属性和方法: {dir(result)}")
        # 这对于探索不熟悉的对象结构非常有用
        
        # ====== 正确访问CallToolResult结果的三种方式 ======
        
        # 方式1: 通过content[0].text访问文本结果
        # content是一个列表，包含TextContent对象，其中text属性存储文本形式的结果
        print(f"\n方式1 - 通过content[0].text获取: {result.content[0].text}")
        assert result.content[0].text == "8"
        # 检查content属性的结构
        print(f"content的类型: {type(result.content)}")
        print(f"content[0]的类型: {type(result.content[0])}")
        
        # 方式2: 通过data属性直接访问返回的数据
        # data属性存储原始数据类型的结果（这里是整数8）
        print(f"\n方式2 - 通过data属性获取: {result.data}")
        print(f"data的类型: {type(result.data)}")  # 这里应该是int类型
        assert result.data == 8
        
        # 方式3: 通过structured_content访问结构化结果
        # structured_content是一个字典，包含了结构化的结果数据
        print(f"\n方式3 - 通过structured_content获取: {result.structured_content['result']}")
        print(f"structured_content的类型: {type(result.structured_content)}")
        # 查看structured_content字典中的所有键
        print(f"structured_content中的所有键: {list(result.structured_content.keys())}")
        assert result.structured_content['result'] == 8
        
        # ====== 常见错误避免 ======
        # 错误1: 尝试将CallToolResult对象作为列表索引访问
        # 错误写法: result[0]  # 会导致 TypeError: 'CallToolResult' object is not subscriptable
        
        # 错误2: 直接访问不存在的属性
        # 错误写法: result.text  # 会导致 AttributeError: 'CallToolResult' object has no attribute 'text'
        
        print("\n所有断言都通过了！")

if __name__ == "__main__":
    # 正确启动异步函数的方法
    # 方法1: 使用asyncio.run()直接运行异步函数
    asyncio.run(test_tool_execution())
    
    # 方法2: 在Python交互式环境中可以这样运行:
    # >>> import asyncio
    # >>> from mcp_test import test_tool_execution
    # >>> asyncio.run(test_tool_execution())
    
    # 方法3: 如果在其他异步函数中，可以使用await关键字:
    # async def main():
    #     await test_tool_execution()
    # asyncio.run(main())
