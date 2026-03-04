#!/usr/bin/env python3
"""
小市值动量+筹码峰策略 - 优化版
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


def calculate_momentum(df, ts_code, trade_date):
    """计算动量评分"""
    stock_data = df[(df['ts_code'] == ts_code) & (df['trade_date'] < trade_date)].sort_values('trade_date').tail(30)
    if len(stock_data) < 20:
        return 0
    
    current_price = stock_data['close'].iloc[-1]
    prices = stock_data['close'].values
    
    score = 0
    
    # 多头排列
    if len(prices) >= 20:
        ma5 = np.mean(prices[-5:])
        ma10 = np.mean(prices[-10:])
        ma20 = np.mean(prices[-20:])
        
        if ma5 > ma10 > ma20:
            score += 0.6
        elif ma5 > ma10:
            score += 0.3
        
        if current_price > ma20:
            score += 0.4
    
    return min(score, 1.0)


def run_optimized(start_year=2021, end_year=2024):
    """优化版"""
    print("="*60)
    print("📊 小市值动量+筹码峰策略 - 优化版")
    print("="*60)
    
    # 测试不同参数
    best_result = None
    best_params = None
    
    for stock_num in [5, 8, 10]:
        for stop_loss in [-0.08, -0.10, -0.15]:
            for min_pe in [0, 10]:
                for max_mv in [50, 80, 100]:
                    result = run_backtest(start_year, end_year, stock_num, stop_loss, min_pe, max_mv)
                    if result:
                        if not best_result or result['annual_return'] > best_result['annual_return']:
                            best_result = result
                            best_params = {'stock_num': stock_num, 'stop_loss': stop_loss, 'min_pe': min_pe, 'max_mv': max_mv}
    
    print(f"\n🏆 最佳参数: {best_params}")
    print(f"\n📈 最佳回测结果")
    print(f"{'='*50}")
    print(f"交易次数: {best_result['n']}")
    print(f"总收益率: {best_result['total_return']*100:.2f}%")
    print(f"年化收益率: {best_result['annual_return']*100:.2f}%")
    print(f"夏普比率: {best_result['sharpe']:.2f}")
    print(f"最大回撤: {best_result['max_drawdown']*100:.2f}%")
    print(f"胜率: {best_result['win_rate']*100:.1f}%")
    
    return best_result, best_params


def run_backtest(start_year, end_year, stock_num, stop_loss, min_pe, max_mv):
    years = list(range(start_year, end_year + 1))
    daily = load_data(years)
    
    if daily is None:
        return None
    
    dates = sorted(daily['trade_date'].unique())
    
    # 每周二调仓
    weekly_dates = [d for d in dates if datetime.strptime(str(d), '%Y%m%d').weekday() == 1]
    
    if len(weekly_dates) < 10:
        return None
    
    weekly_returns = []
    
    for i, trade_date in enumerate(weekly_dates):
        day_df = daily[daily['trade_date'] == trade_date].copy()
        if len(day_df) == 0:
            continue
        
        # 筛选
        day_df['circ_mv_yi'] = day_df['circ_mv'] / 10000
        day_df = day_df[day_df['circ_mv_yi'] <= max_mv]
        day_df = day_df[day_df['circ_mv_yi'] > 0]
        day_df = day_df[day_df['pe'] >= min_pe]
        day_df = day_df[day_df['pe'] < 80]
        
        if len(day_df) == 0:
            continue
        
        # 计算动量评分
        stocks_with_scores = []
        for _, row in day_df.iterrows():
            score = calculate_momentum(daily, row['ts_code'], trade_date)
            mv_rank = day_df.index.get_loc(row.name)
            final_score = -mv_rank + score * 5
            stocks_with_scores.append((row['ts_code'], row['close'], final_score))
        
        stocks_with_scores.sort(key=lambda x: x[2], reverse=True)
        selected = stocks_with_scores[:stock_num]
        
        # 计算收益
        if i + 1 < len(weekly_dates):
            next_date = weekly_dates[i + 1]
            next_df = daily[daily['trade_date'] == next_date]
            
            stock_returns = []
            for ts_code, buy_price, _ in selected:
                stock_start = day_df[day_df['ts_code'] == ts_code]
                stock_end = next_df[next_df['ts_code'] == ts_code]
                
                if len(stock_start) > 0 and len(stock_end) > 0:
                    ret = (stock_end['close'].values[0] - stock_start['close'].values[0]) / stock_start['close'].values[0]
                    stock_returns.append(ret)
            
            if stock_returns:
                avg_ret = np.mean(stock_returns)
                weekly_returns.append(avg_ret)
    
    if not weekly_returns:
        return None
    
    returns = weekly_returns
    total_return = np.prod([1 + r for r in returns]) - 1
    n = len(returns)
    annual_return = (1 + total_return) ** (52 / n) - 1
    
    vol = np.std(returns)
    annual_vol = vol * np.sqrt(52)
    sharpe = (annual_return - 0.03) / annual_vol if annual_vol > 0 else 0
    
    cumprod = np.cumprod([1 + r for r in returns])
    peak = np.maximum.accumulate(cumprod)
    drawdown = (cumprod - peak) / peak
    max_dd = np.min(drawdown)
    
    win_rate = len([r for r in returns if r > 0]) / n
    
    return {
        'total_return': total_return,
        'annual_return': annual_return,
        'sharpe': sharpe,
        'max_drawdown': max_dd,
        'win_rate': win_rate,
        'n': n
    }


if __name__ == '__main__':
    run_optimized(2021, 2024)
