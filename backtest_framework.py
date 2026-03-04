#!/usr/bin/env python3
"""
三策略组合回测框架
基于Backtrader - ETF轮动 + 小市值 + 白马策略
"""
import os
import backtrader as bt
import pandas as pd
import numpy as np
from datetime import datetime

# 数据目录
DATA_DIR = os.path.join(os.path.dirname(__file__), 'data')


def load_index_data(code, start_date='2020-01-01'):
    """加载指数数据"""
    fname = f'{DATA_DIR}/index_{code.replace(".", "_")}.csv'
    
    if not os.path.exists(fname):
        print(f'数据文件不存在: {fname}')
        return None
    
    df = pd.read_csv(fname)
    df['date'] = pd.to_datetime(df['trade_date'], format='%Y%m%d')
    df = df.set_index('date')
    df = df.sort_index()
    df = df[df.index >= start_date]
    
    return df


def run_simple_backtest():
    """运行简化版回测"""
    print("=" * 60)
    print("ETF轮动策略回测")
    print("=" * 60)
    
    # 加载数据
    indices = {
        '000300.SH': '沪深300',
        '000905.SH': '中证500', 
        '000852.SH': '中证1000',
    }
    
    results = {}
    for code, name in indices.items():
        df = load_index_data(code)
        if df is not None:
            # 计算20日动量
            df['momentum'] = df['close'].pct_change(20)
            results[name] = df
    
    if not results:
        print("没有数据")
        return
    
    # 模拟ETF轮动策略
    dates = sorted(set(results['沪深300'].index) & set(results['中证500'].index) & set(results['中证1000'].index))
    print(f"回测区间: {dates[0]} ~ {dates[-1]}")
    print(f"交易日数: {len(dates)}")
    
    # 按20天调仓
    monthly_returns = []
    for i in range(20, len(dates), 20):
        current_date = dates[i]
        
        # 计算各指数动量
        momentum = {}
        for name, df in results.items():
            if current_date in df.index:
                mom = df.loc[current_date, 'momentum']
                if not np.isnan(mom):
                    momentum[name] = mom
        
        if momentum:
            # 选择最强动量
            best = max(momentum, key=momentum.get)
            
            # 计算下20天收益
            if i + 20 < len(dates):
                next_date = dates[i + 20]
                
                for name, df in results.items():
                    if current_date in df.index and next_date in df.index:
                        ret = (df.loc[next_date, 'close'] / df.loc[current_date, 'close']) - 1
                        if name == best:
                            monthly_returns.append({'date': current_date, 'strategy': name, 'return': ret})
    
    if monthly_returns:
        rets = [r['return'] for r in monthly_returns]
        total = np.prod([1 + r for r in rets]) - 1
        annual = (1 + total) ** (len(rets) / 12) - 1
        
        print(f"\nETF轮动策略结果:")
        print(f"  调仓次数: {len(rets)}")
        print(f"  总收益: {total * 100:.2f}%")
        print(f"  年化收益: {annual * 100:.2f}%")
        print(f"  胜率: {len([r for r in rets if r > 0]) / len(rets) * 100:.1f}%")
        
        # 买入持有对比
        hs300 = results['沪深300']
        bh_ret = (hs300['close'].iloc[-1] / hs300['close'].iloc[0]) - 1
        print(f"\n沪深300买入持有: {bh_ret * 100:.2f}%")
        print(f"策略超额: {(total - bh_ret) * 100:.2f}%")
        
        # 调仓详情
        print(f"\n调仓记录:")
        for r in monthly_returns[:10]:
            print(f"  {r['date'].strftime('%Y-%m-%d')}: {r['strategy']} {r['return']*100:+.2f}%")


if __name__ == '__main__':
    run_simple_backtest()
