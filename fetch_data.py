#!/usr/bin/env python3
"""
A股小市值策略数据获取
获取所有A股的日线基本面数据（市值、PE、PB等）
"""
import os
import sys
import pandas as pd
from datetime import datetime, timedelta
import time

# 加载环境变量
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), 'config/.env.tushare'))

TOKEN = os.getenv('TUSHARE_TOKEN')
DATA_DIR = os.path.join(os.path.dirname(__file__), 'data')
os.makedirs(DATA_DIR, exist_ok=True)

def get_pro():
    """初始化tushare pro"""
    import tushare as ts
    pro = ts.pro_api(TOKEN)
    return pro

def fetch_stock_basic():
    """获取A股股票基本信息"""
    print("📥 获取股票基本信息...")
    pro = get_pro()
    
    # 获取所有A股
    df = pro.stock_basic(exchange='', list_status='L', 
                         fields='ts_code,symbol,name,area,industry,list_date,market,exchange')
    
    # 过滤ST股票
    df = df[~df['name'].str.contains('ST|退', na=False)]
    
    output_file = os.path.join(DATA_DIR, 'stock_basic.csv')
    df.to_csv(output_file, index=False)
    print(f"✅ 股票基本信息: {len(df)} 只")
    return df

def fetch_daily_basic_all():
    """
    获取所有A股日线基本面
    包含：市值、PE、PB、换手率等
    """
    print("📥 获取日线基本面数据...")
    pro = get_pro()
    
    # 读取股票列表
    stocks = pd.read_csv(os.path.join(DATA_DIR, 'stock_basic.csv'))
    
    # 全部股票
    all_ts_codes = stocks['ts_code'].tolist()
    print(f"  📊 共 {len(all_ts_codes)} 只股票")
    
    # 分批获取（每次50只）
    batch_size = 50
    all_data = []
    
    for i in range(0, len(all_ts_codes), batch_size):
        batch = all_ts_codes[i:i+batch_size]
        ts_codes = ','.join(batch)
        
        try:
            # 获取最近90天的数据
            df = pro.daily_basic(ts_code=ts_codes, start_date='20260101',
                                fields='ts_code,trade_date,close,pe,pb,ps,total_mv,circ_mv,turnover_rate')
            
            if df is not None and len(df) > 0:
                all_data.append(df)
            
            # 进度显示
            progress = min(i + batch_size, len(all_ts_codes))
            print(f"  进度: {progress}/{len(all_ts_codes)}", end='\r')
            
            # 避免请求过快
            time.sleep(0.5)
            
        except Exception as e:
            print(f"  ⚠️ 批次{i//batch_size}失败: {e}")
            continue
    
    if all_data:
        result = pd.concat(all_data, ignore_index=True)
        
        # 按日期和市值排序
        result = result.sort_values(['trade_date', 'total_mv'], ascending=[False, True])
        
        output_file = os.path.join(DATA_DIR, 'daily_basic_all.csv')
        result.to_csv(output_file, index=False)
        print(f"\n✅ 日线基本面: {len(result)} 条记录 → {output_file}")
        return result
    
    return None

def fetch_index_daily():
    """获取主要指数行情"""
    print("📥 获取指数行情...")
    pro = get_pro()
    
    indices = {
        '000300.SH': '沪深300',
        '000905.SH': '中证500',
        '000852.SH': '中证1000',
        '000001.SH': '上证指数',
        '399001.SZ': '深证成指',
    }
    
    for ts_code, name in indices.items():
        df = pro.index_daily(ts_code=ts_code, start_date='20250101')
        if df is not None and len(df) > 0:
            output_file = os.path.join(DATA_DIR, f'index_{ts_code.replace(".", "_")}.csv')
            df.to_csv(output_file, index=False)
            print(f"  ✅ {name}: {len(df)} 条")
    
    print("✅ 指数数据完成")

def get_small_cap_universe(date=None):
    """
    获取小市值股票池
    默认选流通市值最小的500只
    """
    if date is None:
        date = datetime.now().strftime('%Y%m%d')
    
    df = pd.read_csv(os.path.join(DATA_DIR, 'daily_basic_all.csv'))
    
    # 取最新日期
    latest_date = df['trade_date'].max()
    df_latest = df[df['trade_date'] == latest_date]
    
    # 按流通市值排序，取最小的500只
    df_small = df_latest.nsmallest(500, 'circ_mv')
    
    # 合并股票名称
    basic = pd.read_csv(os.path.join(DATA_DIR, 'stock_basic.csv'))
    df_small = df_small.merge(basic[['ts_code', 'name', 'industry']], on='ts_code', how='left')
    
    # 流通市值转换为亿元
    df_small['circ_mv_yi'] = df_small['circ_mv'] / 10000
    
    output_file = os.path.join(DATA_DIR, f'small_cap_{latest_date}.csv')
    df_small.to_csv(output_file, index=False)
    
    print(f"\n📈 小市值股票池 ({latest_date}):")
    print(f"   总数: {len(df_small)} 只")
    print(f"   平均市值: {df_small['circ_mv_yi'].mean():.1f} 亿元")
    print(f"   最小市值: {df_small['circ_mv_yi'].min():.1f} 亿元")
    print(f"   最大市值: {df_small['circ_mv_yi'].max():.1f} 亿元")
    print(f"\n   前10小市值:")
    print(df_small[['ts_code', 'name', 'circ_mv_yi', 'industry']].head(10).to_string(index=False))
    
    return df_small

def main():
    print("=" * 60)
    print("🚀 A股小市值策略数据系统")
    print("=" * 60)
    
    # 1. 股票基本信息
    fetch_stock_basic()
    
    # 2. 指数数据
    fetch_index_daily()
    
    # 3. 日线基本面（核心！）
    fetch_daily_basic_all()
    
    # 4. 生成小市值股票池
    get_small_cap_universe()
    
    print("\n" + "=" * 60)
    print("🎉 数据层搭建完成!")
    print("=" * 60)

if __name__ == '__main__':
    main()
