#!/usr/bin/env python3
"""
获取小市值股票的财务指标数据
"""
import akshare as ak
import pandas as pd
import os
import time

DATA_DIR = os.path.join(os.path.dirname(__file__), 'data')
os.makedirs(DATA_DIR, exist_ok=True)

def get_stock_list():
    """获取A股股票列表"""
    try:
        df = ak.stock_info_a_code_name()
        return df
    except Exception as e:
        print(f"获取股票列表失败: {e}")
        return None

def get_financial_data(ts_code, max_retries=2):
    """获取单只股票财务数据"""
    for _ in range(max_retries):
        try:
            df = ak.stock_financial_abstract_ths(symbol=ts_code)
            if len(df) > 0:
                df['ts_code'] = ts_code
                return df
        except:
            time.sleep(0.5)
    return None

print("="*60)
print("📊 获取小市值股票财务数据")
print("="*60)

# 获取股票列表
stock_list = get_stock_list()
if stock_list is None:
    print("❌ 无法获取股票列表")
    exit(1)

print(f"共有 {len(stock_list)} 只股票")

# 读取已有的市值数据
daily_files = [f for f in os.listdir(DATA_DIR) if f.startswith('daily_') and f.endswith('.csv')]
if daily_files:
    latest_file = sorted(daily_files)[-1]
    daily = pd.read_csv(f'{DATA_DIR}/{latest_file}')
    # 获取最小市值的股票
    daily['circ_mv_yi'] = daily['circ_mv'] / 10000
    small_cap = daily[daily['circ_mv_yi'] <= 50].copy()
    target_stocks = small_cap.groupby('ts_code').first().reset_index()
    target_codes = target_stocks['ts_code'].tolist()[:100]  # 取前100只
    print(f"选取市值最小的100只股票获取财务数据")
else:
    target_codes = stock_list['code'].tolist()[:100]
    print(f"使用股票列表前100只")

# 获取财务数据
all_financial = []
for i, code in enumerate(target_codes):
    if (i+1) % 10 == 0:
        print(f"进度: {i+1}/{len(target_codes)}")
    
    fin = get_financial_data(code)
    if fin is not None and len(fin) > 0:
        all_financial.append(fin)

if all_financial:
    financial_df = pd.concat(all_financial, ignore_index=True)
    financial_df.to_csv(f'{DATA_DIR}/financial_small_cap.csv', index=False, encoding='utf-8-sig')
    print(f"\n✅ 成功获取 {len(financial_df)} 条财务数据")
    print(f"保存到: {DATA_DIR}/financial_small_cap.csv")
    
    # 显示数据结构
    print("\n数据字段:")
    print(financial_df.columns.tolist())
else:
    print("❌ 没有获取到财务数据")
