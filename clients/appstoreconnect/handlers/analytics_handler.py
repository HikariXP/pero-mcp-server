"""
App Store Connect 分析数据处理器 - 负责销售和下载数据分析
"""

from typing import Any, Optional

from ..models import (ReportFrequency, SalesReportType)
from ...mcp_handler_interface import IMCPHandler

import gzip
import io
from datetime import datetime
import os
import re


class AnalyticsHandler(IMCPHandler):
    """分析数据处理器 - 负责销售报告、下载数据等分析功能"""

    def __init__(self, client):
        self.client = client

    def register_tools(self, mcp: Any) -> None:
        """注册分析数据相关工具"""

        @mcp.tool("get_appstore_sales_report")
        def get_appstore_sales_report_tool(
                report_type: str = "SALES",
                report_subtype: str = "SUMMARY",
                frequency: str = "DAILY",
                report_date: str = ""
        ) -> str:
            """
            下载 AppStore 销售和趋势报告，下载根据您指定的标准过滤的销售和趋势报告。

            Args:
                report_type (str): (Required) The report to download. For more details on each report type see Download and view reports.
                    Possible Values: SALES, PRE_ORDER, NEWSSTAND, SUBSCRIPTION, SUBSCRIPTION_EVENT, SUBSCRIBER, SUBSCRIPTION_OFFER_CODE_REDEMPTION, INSTALLS, FIRST_ANNUAL, WIN_BACK_ELIGIBILITY
                report_subtype (str): (Required) The report sub type to download. For a list of values, see Allowed values based on sales report type table below.
                    Possible Values: SUMMARY, DETAILED, SUMMARY_INSTALL_TYPE, SUMMARY_TERRITORY, SUMMARY_CHANNEL
                frequency (str): (Required) Frequency of the report to download. For a list of values, see Allowed values based on sales report type table below.
                    Possible Values: DAILY, WEEKLY, MONTHLY, YEARLY
                report_date (str): 报告日期，如果是月报告，则格式为YYYY-MM, 如果是日报告，报告格式为YYYY-MM-DD，
                    The report date to download. Specify the date in the YYYY-MM-DD format for all report frequencies except DAILY, which doesn't require a date. For more information, see report availability and storage.
            Returns:
                str: 销售报告内容
            """
            try:
                if not self.client.config:
                    self.client.config = self.client.load_config_from_env()

                # 这里需要vendor_number，从配置中获取
                vendor_number = getattr(self.client.config, 'vendor_number', None)
                if not vendor_number:
                    return "未配置vendor_number，无法获取分析数据"

                report = self.get_sales_report(
                    vendor_number=vendor_number,
                    report_type=SalesReportType(report_type.upper()),
                    report_subtype=report_subtype,
                    frequency=ReportFrequency(frequency.upper()),
                    report_date=report_date
                )
                return report
            except Exception as e:
                return f"获取销售报告失败: {str(e)}"
                
        @mcp.tool("download_appstore_sales_data")
        def download_appstore_sales_data_tool(
                report_type: str = "SALES",
                report_subtype: str = "SUMMARY",
                frequency: str = "DAILY",
                report_date: str = ""
        ) -> str:
            """
            下载并保存 AppStore 销售和趋势报告到本地文件。

            Args:
                report_type (str): (Required) The report to download. For more details on each report type see Download and view reports.
                    Possible Values: SALES, PRE_ORDER, NEWSSTAND, SUBSCRIPTION, SUBSCRIPTION_EVENT, SUBSCRIBER, SUBSCRIPTION_OFFER_CODE_REDEMPTION, INSTALLS, FIRST_ANNUAL, WIN_BACK_ELIGIBILITY
                report_subtype (str): (Required) The report sub type to download. For a list of values, see Allowed values based on sales report type table below.
                    Possible Values: SUMMARY, DETAILED, SUMMARY_INSTALL_TYPE, SUMMARY_TERRITORY, SUMMARY_CHANNEL
                frequency (str): (Required) Frequency of the report to download. For a list of values, see Allowed values based on sales report type table below.
                    Possible Values: DAILY, WEEKLY, MONTHLY, YEARLY
                report_date (str): 报告日期，如果是月报告，则格式为YYYY-MM, 如果是日报告，报告格式为YYYY-MM-DD，
                    The report date to download. Specify the date in the YYYY-MM-DD format for all report frequencies except DAILY, which doesn't require a date. For more information, see report availability and storage.
            Returns:
                str: 保存下来的报告文件的绝对路径
            """
            try:
                if not self.client.config:
                    self.client.config = self.client.load_config_from_env()

                # 这里需要vendor_number，从配置中获取
                vendor_number = getattr(self.client.config, 'vendor_number', None)
                if not vendor_number:
                    return "未配置vendor_number，无法获取分析数据"

                # 获取销售报告数据
                report_data = self.get_sales_report(
                    vendor_number=vendor_number,
                    report_type=SalesReportType(report_type.upper()),
                    report_subtype=report_subtype,
                    frequency=ReportFrequency(frequency.upper()),
                    report_date=report_date
                )
                
                # 保存到本地文件
                abs_path = self._save_data_to_file(report_data, "sale")
                
                return f"销售数据已成功下载并保存到文件: {abs_path}"
            except Exception as e:
                return f"下载并保存销售数据失败: {str(e)}"

        @mcp.tool("get_appstore_finance_report")
        def get_appstore_finance_report_tool(
                region_code: str = "ZZ",
                report_date: str = ""
        ) -> str:
            """
            下载 AppStore 财务报告，获取特定时期的收入和税务信息。

            Args:
                region_code (str): (Required) 报告区域代码。通常使用 "ZZ" 表示全球报告。
                    The region code for the finance report. Use "ZZ" for worldwide reports.
                report_date (str): (Required) 报告日期，格式为 YYYY-MM。
                    The report date in YYYY-MM format. Finance reports are typically available monthly.
            Returns:
                str: 财务报告内容
            """
            try:
                if not self.client.config:
                    self.client.config = self.client.load_config_from_env()

                # 这里需要vendor_number，从配置中获取
                vendor_number = getattr(self.client.config, 'vendor_number', None)
                if not vendor_number:
                    return "未配置vendor_number，无法获取财务数据"

                if not report_date:
                    return "请提供报告日期，格式为 YYYY-MM"

                report = self.get_finance_report(
                    vendor_number=vendor_number,
                    region_code=region_code,
                    report_date=report_date
                )
                return report
            except Exception as e:
                return f"获取财务报告失败: {str(e)}"
                
        @mcp.tool("download_appstore_finance_data")
        def download_appstore_finance_data_tool(
                region_code: str = "ZZ",
                report_date: str = ""
        ) -> str:
            """
            下载并保存 AppStore 财务报告到本地文件。

            Args:
                region_code (str): (Required) 报告区域代码。通常使用 "ZZ" 表示全球报告。
                    The region code for the finance report. Use "ZZ" for worldwide reports.
                report_date (str): (Required) 报告日期，格式为 YYYY-MM。
                    The report date in YYYY-MM format. Finance reports are typically available monthly.
            Returns:
                str: 保存下来的报告文件的绝对路径
            """
            try:
                if not self.client.config:
                    self.client.config = self.client.load_config_from_env()

                # 这里需要vendor_number，从配置中获取
                vendor_number = getattr(self.client.config, 'vendor_number', None)
                if not vendor_number:
                    return "未配置vendor_number，无法获取财务数据"

                if not report_date:
                    return "请提供报告日期，格式为 YYYY-MM"

                # 获取财务报告数据
                report_data = self.get_finance_report(
                    vendor_number=vendor_number,
                    region_code=region_code,
                    report_date=report_date
                )
                
                # 保存到本地文件
                abs_path = self._save_data_to_file(report_data, "finance")
                print(f"保存财务报告到文件: {abs_path}")

                # 读取刚保存的csv文件
                with open(abs_path, 'r', encoding='utf-8') as f:
                    raw_content = f.read()
                print(f"读取财务报告文件内容")
                # 移除特定数据
                cleaned_content = self.remove_total_rows_line(raw_content)
                print(f"移除Total Rows行的内容")
                
                # 按关键字拆离文件
                output1_path = abs_path.replace(".csv", "_part1.csv")
                output2_path = abs_path.replace(".csv", "_part2.csv")
                self.split_file_by_keyword(abs_path, "Country Of Sale", output1_path, output2_path)
                print(f"按Country Of Sale拆离文件: {output1_path}, {output2_path}")
                
                return f"财务数据已成功下载并保存到文件: {abs_path}"
            except Exception as e:
                return f"下载并保存财务数据失败: {str(e)}"

    def register_resources(self, mcp: Any) -> None:
        """注册分析数据相关资源"""
        pass

    def register_prompts(self, mcp: Any) -> None:
        """注册分析数据相关提示"""

        @mcp.prompt("appstore_analytics")
        def appstore_analytics_prompt(
                operation: str = "",
                app_name: str = "",
                vendor_number: str = "",
                date_range: str = ""
        ) -> str:
            """App Store Connect分析数据提示"""
            return f"""App Store Connect 分析数据助手

查询信息:
- 操作类型: {operation}
- 应用名称: {app_name}
- 供应商编号: {vendor_number}
- 日期范围: {date_range}

支持的操作类型:
- get_sales_report: 获取销售报告
- get_finance_report: 获取财务报告
- get_app_analytics: 获取应用分析数据
- get_top_countries: 获取国家排行榜

销售报告类型:
- SALES: 销售报告 (下载和购买)
- SUBSCRIPTION: 订阅报告
- NEWSSTAND: 报刊订阅报告

财务报告:
- FINANCIAL: 财务报告 (收入和税务信息)
- 区域代码: ZZ (全球报告)
- 报告频率: 月度 (YYYY-MM 格式)

频率选项 (销售报告):
- DAILY: 日报告
- WEEKLY: 周报告  
- MONTHLY: 月报报告
- YEARLY: 年报告

使用步骤:
1. 确保已配置vendor_number
2. 选择要查询的应用和日期范围
3. 使用相应的工具获取分析数据

财务报告使用示例:
- get_finance_report(region_code="ZZ", report_date="2024-08")

注意事项:
- 销售数据通常有1-2天延迟
- 财务数据通常有更长的延迟，按月提供
- 需要有效的vendor_number才能获取数据
- 部分数据可能需要特定的权限
- 财务报告包含收入、税务和汇率信息
"""

    # =============================================================================
    # 辅助方法
    # =============================================================================
    
    def _save_data_to_file(self, data: str, data_type: str) -> str:
        """
        通用的数据保存方法
        
        Args:
            data (str): 要保存的数据内容
            data_type (str): 数据类型标识，如'sale'或'finance'
            
        Returns:
            str: 保存的文件的绝对路径
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"AppleData_{data_type}_{timestamp}.csv"
        abs_path = os.path.abspath(filename)
        with io.open(abs_path, 'w', encoding='utf-8') as f:
            f.write(data)
        print(f"{data_type}数据已保存到本地文件: {abs_path}")
        return abs_path

    def remove_total_rows_line(self, content: str) -> str:
        """
        移除内容中形如 "Total_Rows 123" 的行（不区分大小写，数字可为任意正整数）

        Args:
            content (str): 原始文本或 CSV 内容

        Returns:
            str: 移除 Total_Rows 行后的内容
        """
        # 匹配整行：开头可选空白 + Total_Rows（大小写不敏感）+ 空白 + 数字 + 行尾可选空白
        pattern = re.compile(r'^\s*Total_Rows\s+\d+\s*$', re.IGNORECASE | re.MULTILINE)
        cleaned = pattern.sub('', content)
        # 清理可能出现的多余空行
        cleaned = re.sub(r'\n+', '\n', cleaned).strip()
        return cleaned

    def split_file_by_keyword(self, input_path, keyword, output1_path, output2_path, encoding='utf-8'):
        """
        根据关键字拆分文件内容为两个CSV文件
        
        参数：
            input_path (str): 输入文件路径（支持CSV或TXT）
            keyword (str): 用于拆分的关键字
            output1_path (str): 关键字前内容的输出CSV路径
            output2_path (str): 关键字后内容的输出CSV路径
            encoding (str): 文件编码，默认utf-8
        
        返回：
            tuple: (output1_path的绝对路径, output2_path的绝对路径)
        """
        try:
            # 1. 读取输入文件内容（按文本方式读取，兼容CSV和TXT）
            with open(input_path, 'r', encoding=encoding) as f:
                content = f.read()  # 读取全部内容（适合中小文件）
            
            # 2. 检查关键字是否存在
            if keyword not in content:
                print(f"警告：文件中未找到关键字 '{keyword}'，未进行拆分")
                return None, None
            
            # 3. 按关键字拆分内容（移除关键字本身）
            # split只拆分一次，取前两部分（避免关键字多次出现的情况）
            parts = content.split(keyword, 1)  # 1表示最多拆分1次，返回[前缀, 后缀]
            prefix = parts[0]  # 关键字前的内容
            suffix = parts[1]  # 关键字后的内容
            
            # 4. 写入第一个输出文件（关键字前内容）
            with open(output1_path, 'w', encoding=encoding) as f1:
                f1.write(prefix)
            print(f"已生成关键字前内容文件：{output1_path}")
            
            # 5. 写入第二个输出文件（关键字后内容）
            with open(output2_path, 'w', encoding=encoding) as f2:
                f2.write(suffix)
            print(f"已生成关键字后内容文件：{output2_path}")
            
            # 返回两个输出文件的绝对路径
            return os.path.abspath(output1_path), os.path.abspath(output2_path)
            
        except FileNotFoundError:
            print(f"错误：输入文件 '{input_path}' 不存在")
            return None, None
        except Exception as e:
            print(f"处理失败：{str(e)}")
            return None, None
    
    # =============================================================================
    # 业务逻辑方法
    # =============================================================================

    def get_sales_report(
            self,
            vendor_number: str,
            report_type: SalesReportType,
            report_subtype: str,
            frequency: ReportFrequency,
            report_date: str
    ) -> Optional[str]:
        """获取销售报告"""
        data = {
            "filter[frequency]": frequency.value,
            "filter[reportDate]": report_date,
            "filter[reportSubType]": report_subtype,
            "filter[reportType]": report_type.value,
            "filter[vendorNumber]": vendor_number
        }

        # 销售报告API通常返回压缩文件，不是JSON
        response = self.client.make_api_request("salesReports", method="GET", data=data)

        # 处理二进制内容（可能是gzip压缩的CSV）
        raw_content = response["raw_content"]

        # gzip解压缩
        decompressed_data = gzip.decompress(raw_content).decode('utf-8')
        print(f"成功解压缩销售报告，数据长度: {len(decompressed_data)} 字符")
        print(f"解压后销售报告内容前200字符: {decompressed_data[:200]}")

        return decompressed_data

    def get_finance_report(
            self,
            vendor_number: str,
            region_code: str,
            report_date: str
    ) -> Optional[str]:
        """
        获取财务报告
        如果report_type为FINANCE_DETAIL，会报错
        """
        data = {
            "filter[regionCode]": region_code,
            "filter[reportDate]": report_date,
            "filter[reportType]": "FINANCIAL",
            "filter[vendorNumber]": vendor_number
        }

        # 财务报告API通常返回压缩文件，不是JSON
        response = self.client.make_api_request("financeReports", method="GET", data=data)

        # 处理二进制内容（可能是gzip压缩的CSV）
        raw_content = response["raw_content"]

        # gzip解压缩
        decompressed_data = gzip.decompress(raw_content).decode('utf-8')
        print(f"成功解压缩财务报告，数据长度: {len(decompressed_data)} 字符")
        print(f"解压后财务报告内容前200字符: {decompressed_data[:200]}")

        return decompressed_data
