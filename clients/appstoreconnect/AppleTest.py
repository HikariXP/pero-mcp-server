# ai生成出来的不可用代码

import os
import dotenv
import time

# 加载环境变量
dotenv.load_dotenv()

def initialize_client():
    """
    初始化 App Store Connect MCP 客户端和所有处理器
    """
    print("正在初始化 App Store Connect MCP 客户端...")
    
    # 环境变量已经在文件开头加载
    
    # 导入客户端
    import sys
    import os
    # 添加项目根目录到Python路径
    sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
    from clients.appstoreconnect.appstore_connect_mcp_client import AppStoreConnectMCPClient
    
    # 创建客户端实例（这将自动初始化所有处理器）
    client = AppStoreConnectMCPClient()
    
    # 加载配置
    config = client.load_config_from_env()
    if config:
        client.set_config(config)
        print("✅ 配置加载成功！")
    else:
        print("❌ 配置加载失败，请检查环境变量设置。")
    
    print(f"\n已初始化 {len(client.handlers)} 个处理器:")
    for handler_name in client.handlers.keys():
        print(f"- {handler_name}")
    
    return client

def show_menu(client):
    """
    显示交互式菜单，让用户选择要调用的函数
    """
    print("\n" + "="*50)
    print("App Store Connect MCP 客户端测试工具")
    print("="*50)
    
    # 显示处理器列表
    print("\n可用的处理器:")
    handler_names = list(client.handlers.keys())
    for i, name in enumerate(handler_names, 1):
        print(f"{i}. {name}")
    
    print("\n其他选项:")
    print(f"{len(handler_names)+1}. 生成JWT令牌")
    print(f"{len(handler_names)+2}. 退出")
    
    return handler_names

def handle_handler_selection(client, handler_names, choice):
    """
    处理处理器选择
    """
    handler_index = choice - 1
    if 0 <= handler_index < len(handler_names):
        handler_name = handler_names[handler_index]
        handler = client.handlers[handler_name]
        
        print(f"\n正在查看 {handler_name} 的方法...")
        
        # 获取处理器的公共方法（不包括私有方法和继承的方法）
        methods = []
        for name, method in inspect.getmembers(handler, inspect.ismethod):
            if not name.startswith('_') and name not in dir(object):
                methods.append(name)
        
        if methods:
            print(f"\n{handler_name} 可用的方法:")
            for i, method_name in enumerate(methods, 1):
                print(f"{i}. {method_name}")
            
            # 让用户选择方法
            try:
                method_choice = int(input("\n请输入要调用的方法编号: "))
                if 1 <= method_choice <= len(methods):
                    method_name = methods[method_choice - 1]
                    method = getattr(handler, method_name)
                    
                    # 获取方法签名
                    sig = inspect.signature(method)
                    args = []
                    kwargs = {}
                    
                    # 对于没有参数的方法，直接调用
                    if len(sig.parameters) == 0:
                        print(f"\n正在调用 {method_name}()...")
                        try:
                            result = method()
                            print(f"\n方法执行结果: {result}")
                        except Exception as e:
                            print(f"\n方法执行出错: {str(e)}")
                    else:
                        print(f"\n方法 {method_name} 需要参数，暂时不支持自动调用。")
                else:
                    print("无效的选择！")
            except ValueError:
                print("请输入有效的数字！")
        else:
            print(f"未找到 {handler_name} 的可用方法。")
    elif choice == len(handler_names) + 1:
        # 生成JWT令牌
        try:
            print("\n正在生成JWT令牌...")
            token = client.generate_jwt_token()
            print(f"\nJWT令牌:\n{token}")
        except Exception as e:
            print(f"\n生成JWT令牌失败: {str(e)}")

def main():
    """
    主函数
    """
    try:
        # 导入inspect模块用于获取方法信息
        import inspect
        
        # 初始化客户端
        client = initialize_client()
        
        while True:
            # 显示菜单
            handler_names = show_menu(client)
            
            # 获取用户选择
            try:
                choice = int(input("\n请输入选项编号: "))
                
                if choice == len(handler_names) + 2:
                    print("\n感谢使用，再见！")
                    break
                elif 1 <= choice <= len(handler_names) + 1:
                    handle_handler_selection(client, handler_names, choice)
                else:
                    print("无效的选择！")
                
                # 暂停一下让用户看到结果
                input("\n按回车键继续...")
                
            except ValueError:
                print("请输入有效的数字！")
                input("\n按回车键继续...")
    
    except KeyboardInterrupt:
        print("\n\n程序被用户中断，退出...")
    except Exception as e:
        print(f"\n程序运行出错: {str(e)}")

if __name__ == "__main__":
    main()
