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


class FinanceHandler(IMCPHandler):
    """分析数据处理器 - 负责销售报告、下载数据等分析功能"""

    def __init__(self, client):
        self.client = client

    def register_tools(self, mcp: Any) -> None:
        """注册分析数据相关工具"""

        # @mcp.tool("get_appstore_sales_report")
        # 由于直接调用此工具会导致Agent上下文过大，之后不再直接允许MCP过程调用
        def get_appstore_sales_report_tool(
                report_type: str = "SALES",
                report_subtype: str = "SUMMARY",
                frequency: str = "MONTHLY",
                report_date: str = ""
        ) -> str:
            """
            下载 AppStore 销售和趋势报告并获取解压后的数据，下载根据您指定的标准过滤的销售和趋势报告。

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
                str: 销售和趋势报告
            """
            try:
                if not self.client.config:
                    self.client.config = self.client.load_config_from_env()

                # 这里需要vendor_number，从配置中获取
                vendor_number = getattr(self.client.config, 'vendor_number', None)
                if not vendor_number:
                    return "未配置vendor_number，无法获取分析数据"

                report = self.get_sales_report_and_decompress(
                    vendor_number=vendor_number,
                    report_type=SalesReportType(report_type.upper()),
                    report_subtype=report_subtype,
                    frequency=ReportFrequency(frequency.upper()),
                    report_date=report_date
                )
                return report
            except Exception as e:
                error_msg = f"获取销售报告失败: {str(e)}"
                print(error_msg)
                return error_msg
                
        # @mcp.tool("get_appstore_finance_report")
        # 由于直接调用此工具会导致Agent上下文过大，之后不再直接允许MCP过程调用
        def get_appstore_finance_report_tool(
                region_code: str = "ZZ",
                report_date: str = ""
        ) -> str:
            """
            下载 AppStore 财务报告并获取解压后的数据，获取特定时期的收入和税务信息。

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

                report = self.get_finance_report_and_decompress(
                    vendor_number=vendor_number,
                    region_code=region_code,
                    report_date=report_date
                )
                return report
            except Exception as e:
                error_msg = f"获取财务报告失败: {str(e)}"
                print(error_msg)
                return error_msg
                
        
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
                report_data = self.get_sales_report_and_decompress(
                    vendor_number=vendor_number,
                    report_type=SalesReportType(report_type.upper()),
                    report_subtype=report_subtype,
                    frequency=ReportFrequency(frequency.upper()),
                    report_date=report_date
                )
                
                # 构建时间信息
                time_info = ""
                if frequency and report_date:
                    # 将频率和日期格式化为MONTHLY_2025_01这样的格式
                    if frequency.upper() == "MONTHLY" and len(report_date) >= 7:
                        # 月报告格式：MONTHLY_2025_01
                        time_info = f"{frequency.upper()}_{report_date.replace('-', '_')}"
                    elif frequency.upper() == "DAILY" and len(report_date) >= 10:
                        # 日报告格式：DAILY_2025_01_01
                        time_info = f"{frequency.upper()}_{report_date.replace('-', '_')}"
                    elif frequency.upper() == "WEEKLY" and report_date:
                        # 周报告格式：WEEKLY_2025_01_01（假设report_date为周起始日）
                        time_info = f"{frequency.upper()}_{report_date.replace('-', '_')}"
                    elif frequency.upper() == "YEARLY" and report_date:
                        # 年报告格式：YEARLY_2025
                        time_info = f"{frequency.upper()}_{report_date.split('-')[0]}"
                
                # 保存到本地文件
                abs_path = self._save_data_to_file(report_data, "sale", time_info)
                
                return f"销售数据已成功下载并保存到文件: {abs_path}"
            except Exception as e:
                return f"下载并保存销售数据失败: {str(e)}"


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
                report_data = self.get_finance_report_and_decompress(
                    vendor_number=vendor_number,
                    region_code=region_code,
                    report_date=report_date
                )
                
                # 构建时间信息 - 财务报告通常是月度的
                time_info = f"MONTHLY_{report_date.replace('-', '_')}"
                
                # 保存到本地文件
                abs_path = self._save_data_to_file(report_data, "finance", time_info)
                print(f"保存财务报告到文件: {abs_path}")

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
    
    def _save_data_to_file(self, data: str, data_type: str, time_info: str = "") -> str:
        """
        通用的数据保存方法
        
        Args:
            data (str): 要保存的数据内容
            data_type (str): 数据类型标识，如'sale'或'finance'
            time_info (str): 时间信息，格式如'MONTHLY_2025_01'
            
        Returns:
            str: 保存的文件的绝对路径
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        # 如果有时间信息，添加到文件名中
        if time_info:
            filename = f"AppleData_{data_type}_{time_info}_{timestamp}.csv"
        else:
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

    # =============================================================================
    # 业务逻辑方法
    # =============================================================================

    def get_sales_report_and_decompress(
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
        # print(f"解压后销售报告内容前200字符: {decompressed_data[:200]}")

        return decompressed_data

    def decompressed_gzip():
        pass

    def get_finance_report_and_decompress(
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
        # print(f"解压后财务报告内容前200字符: {decompressed_data[:200]}")

        return decompressed_data
