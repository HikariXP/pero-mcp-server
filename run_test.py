import os
import pandas as pd
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

# 获取项目根目录路径
root_dir = os.path.dirname(os.path.abspath(__file__))
# 构造CSV文件路径
csv_path = os.path.join(root_dir, 'AppleData_sale_20251118_140347.csv')

# 读取CSV文件为DataFrame
df = pd.read_csv(csv_path, sep='\t', encoding='utf-8')

# 导入to_postgresql函数
from biz.pandas.to_postgresql import to_postgresql

# 将DataFrame写入PostgreSQL数据库
to_postgresql(dbname="test_db", table_name="apple_sales", df=df, if_exists="replace")