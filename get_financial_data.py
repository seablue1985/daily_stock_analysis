#!/usr/bin/env python3
"""
获取A股财务指标数据
"""
import akshare as ak
import pandas as pd
import os
from datetime import datetime

DATA_DIR = os.path.join(os.path.dirname(__file__), 'data')
os.makedirs(DATA_DIR, exist_ok=True)

print("📊 获取财务指标数据...")

# 获取ROE数据（净资产收益率）
try:
    print("获取ROE数据...")
    roe_df = ak.stock_financial_abstract_ths(symbol="全部A股")
    print(f"ROE数据: {len(roe_df)} 条")
    roe_df.to_csv(f'{DATA_DIR}/financial_roe.csv', index=False, encoding='utf-8-sig')
    print(f"✅ 保存到 {DATA_DIR}/financial_roe.csv")
except Exception as e:
    print(f"❌ 获取ROE失败: {e}")

# 获取主要财务指标
try:
    print("获取主要财务指标...")
    fin_df = ak.stock_financial_analysis_indicator()
    print(f"财务指标: {len(fin_df)} 条")
    fin_df.to_csv(f'{DATA_DIR}/financial_indicator.csv', index=False, encoding='utf-8-sig')
    print(f"✅ 保存到 {DATA_DIR}/financial_indicator.csv")
except Exception as e:
    print(f"❌ 获取财务指标失败: {e}")

# 获取净利润数据
try:
    print("获取净利润数据...")
    profit_df = ak.stock_profit_ths(symbol="A股")
    print(f"净利润: {len(profit_df)} 条")
    profit_df.to_csv(f'{DATA_DIR}/financial_profit.csv', index=False, encoding='utf-8-sig')
    print(f"✅ 保存到 {DATA_DIR}/financial_profit.csv")
except Exception as e:
    print(f"❌ 获取净利润失败: {e}")

print("\n📋 数据字段预览:")
try:
    df = pd.read_csv(f'{DATA_DIR}/financial_indicator.csv')
    print(df.columns.tolist()[:10])
    print(df.head(2))
except:
    pass
