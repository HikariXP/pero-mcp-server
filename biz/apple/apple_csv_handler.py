# 已验证有效
# AppleStore财务报告专攻
# 相关文档参考:https://developer.apple.com/documentation/appstoreconnectapi/get-v1-financereports
import pandas as pd
from io import StringIO
import os
import logging

def read_csv_or_txt_file(file_path):
    """
    读取CSV或TXT文件到pandas DataFrame
    
    Args:
        file_path (str): 文件路径，支持.csv和.txt扩展名
    
    Returns:
        pd.DataFrame: 读取的数据框
    
    Raises:
        FileNotFoundError: 文件不存在
        ValueError: 文件格式不支持或读取失败
    """
    # 检查文件是否存在
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"文件不存在: {file_path}")
    
    # 检查文件扩展名
    _, ext = os.path.splitext(file_path)
    ext = ext.lower()
    
    if ext not in ['.csv', '.txt']:
        raise ValueError(f"不支持的文件格式: {ext}，仅支持.csv和.txt文件")
    
    try:
        # 尝试使用不同的分隔符读取文件
        # 首先尝试自动检测分隔符
        df = pd.read_csv(file_path, encoding='utf-8')
        return df
    except Exception as e:
        try:
            # 如果自动检测失败，尝试使用制表符分隔
            df = pd.read_csv(file_path, sep='\t', encoding='utf-8')
            return df
        except Exception:
            # 如果仍然失败，尝试使用逗号分隔
            try:
                df = pd.read_csv(file_path, sep=',', encoding='utf-8')
                return df
            except Exception:
                # 所有尝试都失败，记录错误并抛出异常
                logging.error(f"读取文件失败: {file_path}, 错误: {str(e)}")
                raise ValueError(f"无法读取文件 {file_path}，请检查文件格式")

def split_broken_csv(file_path):
    # 读取文件所有行（去除空行和前后空白）
    with open(file_path, 'r', encoding='utf-8') as f:
        lines = [line.strip() for line in f if line.strip()]
    
    # 定位Total_Rows所在行的索引
    total_row_index = None
    for i, line in enumerate(lines):
        if line.startswith('Total_Rows'):
            total_row_index = i
            break
    if total_row_index is None:
        raise ValueError("未找到'Total_Rows'行")
    
    # 提取第一部分（表头+数据）
    part1_header = lines[0]
    part1_data = lines[1:total_row_index]  # 从第2行到Total_Rows行之前
    part1_content = part1_header + '\n' + '\n'.join(part1_data)
    df_part1 = pd.read_csv(StringIO(part1_content), sep='\t')  # 制表符分隔
    
    # 提取Total_Rows数值
    total_rows = lines[total_row_index].split('\t')[1]  # 按制表符分割后取第2个元素
    
    # 提取第二部分（表头+数据）
    part2_header = lines[total_row_index + 1]
    part2_data = lines[total_row_index + 2:]  # 从Total_Rows下一行的下一行开始
    part2_content = part2_header + '\n' + '\n'.join(part2_data)
    df_part2 = pd.read_csv(StringIO(part2_content), sep='\t')
    
    return df_part1, total_rows, df_part2

def read_csv_into_parts(file_path):
    """
    读取CSV文件并返回三个部分的数据
    
    参考split_broken_csv方法的实现，读取目标CSV文件后，返回数据的三个部分：
    1. 第一部分数据框（主要数据）
    2. 总行数信息
    3. 第二部分数据框（补充数据）
    
    Args:
        file_path (str): CSV文件路径
    
    Returns:
        tuple: (df_part1, total_rows, df_part2)
    
    Raises:
        FileNotFoundError: 文件不存在
        ValueError: 文件格式不正确或未找到必要的标记行
    """
    # 检查文件是否存在
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"文件不存在: {file_path}")
    
    # 检查文件扩展名
    _, ext = os.path.splitext(file_path)
    ext = ext.lower()
    
    if ext not in ['.csv', '.txt']:
        raise ValueError(f"不支持的文件格式: {ext}，仅支持.csv和.txt文件")
    
    try:
        # 读取文件所有行（去除空行和前后空白）
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = [line.strip() for line in f if line.strip()]
        
        # 定位Total_Rows所在行的索引
        total_row_index = None
        for i, line in enumerate(lines):
            if line.startswith('Total_Rows'):
                total_row_index = i
                break
        
        if total_row_index is None:
            # 如果没有找到Total_Rows标记，尝试将文件分为两部分
            # 默认使用文件中间位置作为分割点
            mid_point = len(lines) // 2
            
            # 第一部分：从头开始到中间点
            if mid_point > 0:
                part1_header = lines[0]
                part1_data = lines[1:mid_point]
                part1_content = part1_header + '\n' + '\n'.join(part1_data)
                df_part1 = pd.read_csv(StringIO(part1_content), sep='\t')
                
                # 总行数设置为第一部分数据行数
                total_rows = str(len(df_part1))
                
                # 第二部分：从中间点开始到结束
                if mid_point < len(lines):
                    part2_header = lines[mid_point]
                    part2_data = lines[mid_point + 1:]
                    part2_content = part2_header + '\n' + '\n'.join(part2_data)
                    df_part2 = pd.read_csv(StringIO(part2_content), sep='\t')
                else:
                    # 如果没有第二部分，返回空数据框
                    df_part2 = pd.DataFrame()
            else:
                # 如果文件内容太少，返回空数据框
                df_part1 = pd.DataFrame()
                total_rows = '0'
                df_part2 = pd.DataFrame()
        else:
            # 按照split_broken_csv的逻辑处理有Total_Rows标记的文件
            # 提取第一部分（表头+数据）
            part1_header = lines[0]
            part1_data = lines[1:total_row_index]
            part1_content = part1_header + '\n' + '\n'.join(part1_data)
            df_part1 = pd.read_csv(StringIO(part1_content), sep='\t')
            
            # 提取Total_Rows数值
            total_rows = lines[total_row_index].split('\t')[1]
            
            # 提取第二部分（表头+数据）
            if total_row_index + 1 < len(lines):
                part2_header = lines[total_row_index + 1]
                part2_data = lines[total_row_index + 2:] if total_row_index + 2 < len(lines) else []
                part2_content = part2_header + '\n' + '\n'.join(part2_data)
                df_part2 = pd.read_csv(StringIO(part2_content), sep='\t')
            else:
                df_part2 = pd.DataFrame()
        
        return df_part1, total_rows, df_part2
        
    except Exception as e:
        logging.error(f"读取CSV文件失败: {file_path}, 错误: {str(e)}")
        raise ValueError(f"无法读取并分割CSV文件 {file_path}，请检查文件格式")

# 使用示例
if __name__ == "__main__":
    # 测试新增的read_csv_or_txt_file方法
    print("===== 测试read_csv_or_txt_file方法 =====")
    
    # 测试成功场景 - 使用项目中的示例CSV文件
    try:
        # 尝试使用项目中可能存在的CSV文件
        sample_files = [
            "d:/UnityProject/pero-mcp-server/AppleData_finance_20251113_182925.csv",
            "d:/UnityProject/pero-mcp-server/AppleData_finance_20251114_092001.csv",
            "d:/UnityProject/pero-mcp-server/AppleData_sale_20251114_114248.csv"
        ]
        
        # 找到第一个存在的示例文件
        test_file = None
        for file in sample_files:
            if os.path.exists(file):
                test_file = file
                break
        
        if test_file:
            print(f"测试成功场景 - 读取文件: {test_file}")
            df = read_csv_or_txt_file(test_file)
            print(f"成功读取文件，数据形状: {df.shape}")
            print("数据前5行:")
            print(df.head())
        else:
            print("警告: 未找到示例CSV文件进行测试，请手动指定文件路径")
    except Exception as e:
        print(f"成功场景测试失败: {str(e)}")
    
    # 测试错误场景 - 文件不存在
    print("\n测试错误场景 - 文件不存在")
    try:
        read_csv_or_txt_file("non_existent_file.csv")
    except FileNotFoundError as e:
        print(f"正确捕获异常: {str(e)}")
    except Exception as e:
        print(f"捕获到意外异常: {str(e)}")
    
    # 测试错误场景 - 不支持的文件格式
    print("\n测试错误场景 - 不支持的文件格式")
    try:
        # 创建一个临时的不支持格式的文件（如果不存在）
        unsupported_file = "test.unsupported"
        with open(unsupported_file, 'w') as f:
            f.write("test")
        
        read_csv_or_txt_file(unsupported_file)
        # 清理临时文件
        os.remove(unsupported_file)
    except ValueError as e:
        print(f"正确捕获异常: {str(e)}")
        # 确保清理临时文件
        if os.path.exists(unsupported_file):
            os.remove(unsupported_file)
    except Exception as e:
        print(f"捕获到意外异常: {str(e)}")
        # 确保清理临时文件
        if os.path.exists(unsupported_file):
            os.remove(unsupported_file)
    
    # 原始split_broken_csv方法的使用示例
    print("\n===== 原始split_broken_csv方法示例 =====")
    file_path = "your_path.csv"  # 替换为实际的CSV文件路径
    print(f"请将'your_path.csv'替换为实际的CSV文件路径以测试split_broken_csv方法")
    
    # 新增read_csv_into_parts方法的使用示例
    print("\n===== 新增read_csv_into_parts方法示例 =====")
    try:
        # 使用与之前相同的示例文件测试新方法
        if test_file:
            print(f"测试新方法 - 读取文件: {test_file}")
            df_part1, total_rows, df_part2 = read_csv_into_parts(test_file)
            print(f"成功读取并分割文件")
            print(f"第一部分数据形状: {df_part1.shape}")
            print(f"总行数信息: {total_rows}")
            print(f"第二部分数据形状: {df_part2.shape}")
            
            # 如果第一部分不为空，显示前几行
            if not df_part1.empty:
                print("\n第一部分数据前3行:")
                print(df_part1.head(3))
            
            # 如果第二部分不为空，显示前几行
            if not df_part2.empty:
                print("\n第二部分数据前3行:")
                print(df_part2.head(3))
        else:
            print("警告: 未找到示例CSV文件进行测试，请手动指定文件路径")
    except Exception as e:
        print(f"新方法测试失败: {str(e)}")