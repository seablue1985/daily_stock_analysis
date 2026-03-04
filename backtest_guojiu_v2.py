#!/usr/bin/env python3
"""
国九小市值策略优化版 - 基于聚宽zycash策略
"""
import os
import pandas as pd
import numpy as np
from datetime import datetime

DATA_DIR = os.path.join(os.path.dirname(__file__), 'data')


def load_data(years):
    dfs = []
    for year in years:
        path = f'{DATA_DIR}/daily_{year}.csv'
        if os.path.exists(path):
            dfs.append(pd.read_csv(path))
    return pd.concat(dfs, ignore_index=True) if dfs else None


def run_strategy(start_year=2021, end_year=2024):
    print("="*60)
    print("国九小市值策略优化版")
    print("="*60)
    
    MAX_MV = 50
    TOP_N = 25
    STOP_LOSS = -0.10
    
    years = list(range(start_year, end_year + 1))
    daily = load_data(years)
    
    if daily is None:
        print("没有数据")
        return
    
    dates = sorted(daily['trade_date'].unique())
    print(f"数据范围: {dates[0]} ~ {dates[-1]}")
    
    trade_dates = []
    for d in dates:
        try:
            dt = datetime.strptime(str(d), '%Y%m%d')
            if dt.weekday() == 1:
                trade_dates.append(d)
        except:
            pass
    
    active_dates = [d for d in trade_dates if (d // 100) % 100 not in [1, 4]]
    print(f"调仓日: {len(active_dates)}")
    
    def select_stocks(df, top_n=25):
        df = df.copy()
        df['mv'] = df['circ_mv'] / 10000
        df = df[(df['mv'] <= MAX_MV) & (df['mv'] > 0) & (df['pe'] > 0) & (df['pe'] < 80)]
        df = df[~df['ts_code'].str.contains('ST|ST', na=False)]
        return df.sort_values('mv').head(top_n)
    
    returns_list = []
    prev_positions = {}
    
    for i, trade_date in enumerate(active_dates):
        day_df = daily[daily['trade_date'] == trade_date]
        if len(day_df) == 0:
            continue
        
        selected = select_stocks(day_df, TOP_N)
        
        if i + 1 < len(active_dates):
            next_date = active_dates[i + 1]
            next_df = daily[daily['trade_date'] == next_date]
            
            ret_list = []
            for _, row in selected.iterrows():
                s = next_df[next_df['ts_code'] == row['ts_code']]
                if len(s) > 0:
                    ret = (s['close'].values[0] - row['close']) / row['close']
                    ret_list.append(ret)
            
            if ret_list:
                returns_list.append(np.mean(ret_list))
            
            prev_positions = {row['ts_code']: row['close'] for _, row in selected.iterrows()}
    
    if not returns_list:
        print("无收益数据")
        return
    
    total = np.prod([1+r for r in returns_list]) - 1
    n = len(returns_list)
    annual = (1+total) ** (52/n) - 1
    vol = np.std(returns_list)
    annual_vol = vol * np.sqrt(52)
    sharpe = (annual - 0.03) / annual_vol if annual_vol > 0 else 0
    
    cumprod = np.cumprod([1+r for r in returns_list])
    peak = np.maximum.accumulate(cumprod)
    dd = (cumprod - peak) / peak
    max_dd = np.min(dd)
    
    win = len([r for r in returns_list if r > 0]) / n
    
    print(f"\n回测结果")
    print(f"调仓次数: {n}")
    print(f"总收益率: {total*100:.2f}%")
    print(f"年化收益率: {annual*100:.2f}%")
    print(f"夏普比率: {sharpe:.2f}")
    print(f"最大回撤: {max_dd*100:.2f}%")
    print(f"胜率: {win*100:.1f}%")
    
    return {'annual_return': annual, 'sharpe': sharpe, 'max_drawdown': max_dd}


if __name__ == '__main__':
    run_strategy(2021, 2024)
