#!/usr/bin/env python3
"""
分批下载A股历史数据
每年下载，一次100只
"""
import tushare as ts
import pandas as pd
import os
import time
from dotenv import load_dotenv

load_dotenv('/Users/ling/.openclaw/workspace/quant_system/config/.env.tushare')
pro = ts.pro_api(os.getenv('TUSHARE_TOKEN'))

DATA_DIR = '/Users/ling/.openclaw/workspace/quant_system/data'

def download_year(year, max_stocks=300):
    """下载指定年份的数据"""
    print(f"\n{'='*50}")
    print(f"下载 {year} 年数据 (最多{max_stocks}只)")
    print(f"{'='*50}")
    
    # 读取股票列表
    stocks = pd.read_csv(f'{DATA_DIR}/stock_basic.csv')['ts_code'].tolist()[:max_stocks]
    
    all_data = []
    for i, code in enumerate(stocks):
        try:
            df = pro.daily_basic(
                ts_code=code, 
                start_date=f'{year}0101', 
                end_date=f'{year}1231',
                fields='ts_code,trade_date,close,pe,total_mv,circ_mv'
            )
            if df is not None and len(df) > 0:
                all_data.append(df)
        except Exception as e:
            pass
        
        if (i+1) % 50 == 0:
            print(f"  进度: {i+1}/{len(stocks)}")
        
        time.sleep(0.15)  # 避免限速
    
    if all_data:
        result = pd.concat(all_data, ignore_index=True)
        result.to_csv(f'{DATA_DIR}/daily_{year}.csv', index=False)
        print(f"  完成: {len(result):,} 条, {result['ts_code'].nunique()} 只股票")
        return result
    else:
        print("  无数据")
        return None

if __name__ == '__main__':
    # 逐年下载
    for year in [2024, 2023, 2022, 2021, 2020, 2019]:
        download_year(year, max_stocks=300)
    
    print("\n全部完成!")
