import pandas as pd
import hashlib

def compute_row_hash(df, target_cols, sep="|||", hash_algorithm="md5"):
    """
    为DataFrame添加或更新hash列，基于指定列的值计算每行的哈希值
    
    参数:
        df (pd.DataFrame): 目标DataFrame
        target_cols (list): 用于计算哈希的列名列表（如["name","age"]）
        sep (str): 连接不同列值的分隔符，避免值拼接歧义（默认"|||"）
        hash_algorithm (str): 哈希算法（如"md5"、"sha256"等，默认"md5"）
    
    返回:
        pd.DataFrame: 已添加/更新hash列的DataFrame
    """
    # 检查目标列是否都存在于DataFrame中
    missing_cols = [col for col in target_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"以下列在DataFrame中不存在: {missing_cols}")
    
    # 检查哈希算法是否支持
    if hash_algorithm not in hashlib.algorithms_available:
        raise ValueError(f"不支持的哈希算法: {hash_algorithm}，支持的算法: {sorted(hashlib.algorithms_available)}")
    
    # 若不存在hash列则创建
    if "hash" not in df.columns:
        df["hash"] = ""
    
    # 定义单行哈希计算逻辑
    def calculate_hash(row):
        # 处理缺失值（NaN/None），统一转换为字符串"NaN"
        values = []
        for col in target_cols:
            val = row[col]
            if pd.isna(val):
                values.append("NaN")
            else:
                # 转换为字符串（处理数字/布尔等类型）
                values.append(str(val))
        
        # 用分隔符连接所有值，避免歧义（如"a+b"和"a|b"用"+"分隔会冲突）
        combined_str = sep.join(values)
        
        # 计算哈希值（需先编码为bytes）
        hash_obj = hashlib.new(hash_algorithm, combined_str.encode("utf-8"))
        return hash_obj.hexdigest()
    
    # 应用到所有行，更新hash列
    df["hash"] = df.apply(calculate_hash, axis=1)
    
    return df