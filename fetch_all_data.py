#!/usr/bin/env python3
"""
A股完整日线基本面数据获取
优化版：稳健获取5000+只股票数据
"""
import os
import pandas as pd
import time
from datetime import datetime

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), 'config/.env.tushare'))

import tushare as ts
TOKEN = os.getenv('TUSHARE_TOKEN')
pro = ts.pro_api(TOKEN)

DATA_DIR = os.path.join(os.path.dirname(__file__), 'data')

def fetch_all_daily_basic():
    """获取所有A股日线基本面 - 稳健版"""
    print("📥 获取完整日线基本面数据...")
    
    # 读取股票列表
    stocks = pd.read_csv(f'{DATA_DIR}/stock_basic.csv')
    all_ts_codes = stocks['ts_code'].tolist()
    
    print(f"  📊 共 {len(all_ts_codes)} 只股票")
    
    # 分批获取
    batch_size = 50
    all_data = []
    failed = []
    
    for i in range(0, len(all_ts_codes), batch_size):
        batch = all_ts_codes[i:i+batch_size]
        
        try:
            df = pro.daily_basic(
                ts_code=','.join(batch),
                start_date='20250101',
                fields='ts_code,trade_date,close,pe,pb,ps,total_mv,circ_mv,turnover_rate'
            )
            
            if df is not None and len(df) > 0:
                all_data.append(df)
            
        except Exception as e:
            failed.extend(batch)
        
        # 进度
        progress = min(i + batch_size, len(all_ts_codes))
        print(f"  进度: {progress}/{len(all_ts_codes)}", end='\r')
        
        # 避免触发限速
        time.sleep(0.3)
    
    print(f"\n  完成! 失败: {len(failed)} 只")
    
    if all_data:
        result = pd.concat(all_data, ignore_index=True)
        
        # 保存
        output_file = f'{DATA_DIR}/daily_basic_all.csv'
        result.to_csv(output_file, index=False)
        
        print(f"\n✅ 日线基本面数据获取完成!")
        print(f"   总记录: {len(result):,}")
        print(f"   股票数: {result['ts_code'].nunique()}")
        print(f"   保存至: {output_file}")
        
        return result
    
    return None

def generate_small_cap_pool():
    """生成小市值股票池"""
    print("\n📈 生成小市值股票池...")
    
    df = pd.read_csv(f'{DATA_DIR}/daily_basic_all.csv')
    
    # 最新日期
    latest_date = df['trade_date'].max()
    df_latest = df[df['trade_date'] == str(latest_date)].copy()
    
    # 按流通市值排序，取最小500只
    df_small = df_latest.nsmallest(500, 'circ_mv').copy()
    
    # 合并名称
    basic = pd.read_csv(f'{DATA_DIR}/stock_basic.csv')
    df_small = df_small.merge(basic[['ts_code', 'name', 'industry']], on='ts_code', how='left')
    
    # 转换市值单位（万元→亿元）
    df_small['circ_mv_yi'] = df_small['circ_mv'] / 10000
    
    # 保存
    output = f'{DATA_DIR}/small_cap_{latest_date}.csv'
    df_small.to_csv(output, index=False)
    
    print(f"\n{'='*50}")
    print(f"小市值股票池 ({latest_date})")
    print(f"{'='*50}")
    print(f"股票数量: {len(df_small)}")
    print(f"平均市值: {df_small['circ_mv_yi'].mean():.1f} 亿元")
    print(f"市值范围: {df_small['circ_mv_yi'].min():.1f} ~ {df_small['circ_mv_yi'].max():.1f} 亿元")
    print(f"\n前20只小市值:")
    print(df_small[['ts_code', 'name', 'circ_mv_yi', 'pe', 'industry']].head(20).to_string(index=False))
    
    return df_small

def main():
    print("=" * 60)
    print("🚀 完整A股数据获取")
    print("=" * 60)
    
    # 获取完整日线基本面
    fetch_all_daily_basic()
    
    # 生成小市值股票池
    generate_small_cap_pool()
    
    print("\n" + "=" * 60)
    print("✅ 数据层搭建完成!")
    print("=" * 60)

if __name__ == '__main__':
    main()
