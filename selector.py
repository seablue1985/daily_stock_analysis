#!/usr/bin/env python3
"""
小市值选股模块
基于A股数据的选股策略
"""
import os
import pandas as pd
from datetime import datetime

DATA_DIR = os.path.join(os.path.dirname(__file__), 'data')

def load_data():
    """加载数据"""
    basic = pd.read_csv(f'{DATA_DIR}/stock_basic.csv')
    
    # 优先使用完整数据，如果没有则用500只样本
    if os.path.exists(f'{DATA_DIR}/daily_basic_all.csv'):
        daily = pd.read_csv(f'{DATA_DIR}/daily_basic_all.csv')
    else:
        daily = pd.read_csv(f'{DATA_DIR}/daily_basic_500.csv')
        print(f"⚠️ 使用样本数据 (500只)")
    
    return basic, daily

def get_small_cap(date=None, top_n=50, max_mv=50):
    """
    小市值选股
    
    参数:
        date: 日期，默认最新
        top_n: 选取前N只最小市值
        max_mv: 最大流通市值（亿元）
    
    返回:
        DataFrame: 选中的股票
    """
    basic, daily = load_data()
    
    # 最新日期 (可能是整数或字符串)
    if date is None:
        date = int(daily['trade_date'].max()) if daily['trade_date'].dtype == 'int64' else daily['trade_date'].max()
    
    df = daily[daily['trade_date'] == date].copy()
    
    # 流通市值转换为亿元
    df['circ_mv_yi'] = df['circ_mv'] / 10000
    
    # 过滤市值
    df = df[df['circ_mv_yi'] <= max_mv]
    
    # 按市值排序
    df = df.sort_values('circ_mv_yi').head(top_n)
    
    # 合并名称和行业
    df = df.merge(basic[['ts_code', 'name', 'industry', 'area']], on='ts_code', how='left')
    
    return df

def filter_by_pe(df, min_pe=0, max_pe=50):
    """按PE过滤"""
    return df[(df['pe'] >= min_pe) & (df['pe'] <= max_pe)]

def filter_by_industry(df, industries):
    """按行业过滤"""
    return df[df['industry'].isin(industries)]

def momentum_filter(df, days=20, pct=-5):
    """
    动量过滤：过滤近N天涨幅过大的股票
    """
    # TODO: 需要历史数据计算
    return df

def generate_signals():
    """
    生成选股信号
    """
    print("📈 小市值策略选股")
    print("=" * 50)
    
    # 获取小市值股票
    stocks = get_small_cap(top_n=100, max_mv=50)
    
    print(f"\n原始候选: {len(stocks)} 只")
    
    # 基础过滤
    stocks = stocks[stocks['pe'] > 0]  # 排除亏损
    stocks = stocks[stocks['pe'] < 80]  # PE太高的不要
    
    print(f"PE过滤后: {len(stocks)} 只")
    
    # 按行业分散
    # 取每个行业市值最小的
    result = stocks.sort_values('circ_mv_yi').groupby('industry').head(2)
    result = result.sort_values('circ_mv_yi').head(30)
    
    print(f"\n最终选股: {len(result)} 只")
    print("\n股票池:")
    print(result[['ts_code', 'name', 'industry', 'circ_mv_yi', 'pe']].to_string(index=False))
    
    # 保存结果
    result.to_csv('data/selector_result.csv', index=False)
    print(f"\n✅ 结果已保存至 data/selector_result.csv")
    
    return result

if __name__ == '__main__':
    generate_signals()
