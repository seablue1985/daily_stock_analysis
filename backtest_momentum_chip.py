#!/usr/bin/env python3
"""
小市值动量+筹码峰策略 (简化版)
基于聚宽社区策略改编
"""
import os
import pandas as pd
import numpy as np

DATA_DIR = os.path.join(os.path.dirname(__file__), 'data')


def load_data(years):
    """加载数据"""
    dfs = []
    for year in years:
        path = f'{DATA_DIR}/daily_{year}.csv'
        if os.path.exists(path):
            dfs.append(pd.read_csv(path))
    return pd.concat(dfs, ignore_index=True) if dfs else None


def calculate_momentum_score(df, ts_code, trade_date):
    """计算动量评分"""
    # 获取历史数据
    stock_data = df[(df['ts_code'] == ts_code) & (df['trade_date'] < trade_date)].sort_values('trade_date').tail(30)
    if len(stock_data) < 20:
        return 0
    
    current_price = stock_data['close'].iloc[-1]
    
    score = 0
    
    # 1. 动量评分：近期涨幅
    prices = stock_data['close'].values
    if len(prices) >= 20:
        ma5 = np.mean(prices[-5:])
        ma10 = np.mean(prices[-10:])
        ma20 = np.mean(prices[-20:])
        
        # 多头排列
        if ma5 > ma10 > ma20:
            score += 0.5
        elif ma5 > ma10:
            score += 0.3
        
        # 站上20日均线
        if current_price > ma20:
            score += 0.3
    
    # 2. 近期涨幅
    if len(prices) >= 10:
        ret_10 = (current_price - prices[-10]) / prices[-10]
        if ret_10 > 0.05:
            score += 0.2
    
    return min(score, 1.0)


def run_strategy(start_year=2021, end_year=2024):
    """运行小市值动量+筹码峰策略"""
    print("="*60)
    print("📊 小市值动量+筹码峰策略")
    print("="*60)
    
    # 参数
    STOCK_NUM = 7  # 持仓数量
    STOP_LOSS = -0.12  # 止损12%
    
    years = list(range(start_year, end_year + 1))
    daily = load_data(years)
    
    if daily is None:
        print("❌ 没有数据")
        return
    
    dates = sorted(daily['trade_date'].unique())
    print(f"数据范围: {dates[0]} ~ {dates[-1]}")
    
    # 按周回测 (每周调仓)
    from datetime import datetime
    weekly_dates = []
    for d in dates:
        try:
            dt = datetime.strptime(str(d), '%Y%m%d')
            if dt.weekday() == 1:  # 周二
                weekly_dates.append(d)
        except:
            pass
    
    print(f"调仓日数量: {len(weekly_dates)}")
    
    # 回测
    prev_positions = {}
    weekly_returns = []
    
    for i, trade_date in enumerate(weekly_dates):
        # 获取当日数据
        day_df = daily[daily['trade_date'] == trade_date].copy()
        if len(day_df) == 0:
            continue
        
        # 基本筛选
        day_df['circ_mv_yi'] = day_df['circ_mv'] / 10000
        day_df = day_df[day_df['circ_mv_yi'] <= 100]  # 小市值
        day_df = day_df[day_df['circ_mv_yi'] > 0]
        day_df = day_df[day_df['pe'] > 0]  # 盈利
        day_df = day_df[day_df['pe'] < 80]
        
        if len(day_df) == 0:
            continue
        
        # 计算动量评分
        stocks_with_scores = []
        for _, row in day_df.iterrows():
            score = calculate_momentum_score(daily, row['ts_code'], trade_date)
            # 市值排名 + 动量评分
            mv_rank = day_df.index.get_loc(row.name)
            final_score = -mv_rank + score * 5
            stocks_with_scores.append((row['ts_code'], row['close'], final_score))
        
        # 按评分排序
        stocks_with_scores.sort(key=lambda x: x[2], reverse=True)
        
        # 选前N只
        selected = stocks_with_scores[:STOCK_NUM * 2]
        
        # 计算收益
        if i + 1 < len(weekly_dates):
            next_date = weekly_dates[i + 1]
            next_df = daily[daily['trade_date'] == next_date]
            
            stock_returns = []
            for ts_code, buy_price, score in selected:
                stock_start = day_df[day_df['ts_code'] == ts_code]
                stock_end = next_df[next_df['ts_code'] == ts_code]
                
                if len(stock_start) > 0 and len(stock_end) > 0:
                    ret = (stock_end['close'].values[0] - stock_start['close'].values[0]) / stock_start['close'].values[0]
                    stock_returns.append(ret)
            
            if stock_returns:
                avg_ret = np.mean(stock_returns)
                weekly_returns.append({'date': trade_date, 'return': avg_ret})
            
            # 更新持仓
            prev_positions = {}
            for ts_code, close_price, score in selected[:STOCK_NUM]:
                prev_positions[ts_code] = close_price
    
    # 统计结果
    if not weekly_returns:
        print("❌ 没有收益数据")
        return
    
    returns = [r['return'] for r in weekly_returns]
    total_return = np.prod([1 + r for r in returns]) - 1
    n = len(returns)
    annual_return = (1 + total_return) ** (52 / n) - 1
    
    vol = np.std(returns)
    annual_vol = vol * np.sqrt(52)
    sharpe = (annual_return - 0.03) / annual_vol if annual_vol > 0 else 0
    
    # 最大回撤
    cumprod = np.cumprod([1 + r for r in returns])
    peak = np.maximum.accumulate(cumprod)
    drawdown = (cumprod - peak) / peak
    max_dd = np.min(drawdown)
    
    win_rate = len([r for r in returns if r > 0]) / n
    
    print(f"\n📈 回测结果")
    print(f"{'='*50}")
    print(f"交易次数: {n}")
    print(f"总收益率: {total_return*100:.2f}%")
    print(f"年化收益率: {annual_return*100:.2f}%")
    print(f"年化波动率: {annual_vol*100:.2f}%")
    print(f"夏普比率: {sharpe:.2f}")
    print(f"最大回撤: {max_dd*100:.2f}%")
    print(f"胜率: {win_rate*100:.1f}%")
    
    print(f"\n📉 最近收益:")
    for r in weekly_returns[-10:]:
        print(f"  {r['date']}: {r['return']*100:+.2f}%")
    
    return {
        'total_return': total_return,
        'annual_return': annual_return,
        'sharpe': sharpe,
        'max_drawdown': max_dd,
        'win_rate': win_rate
    }


if __name__ == '__main__':
    run_strategy(2021, 2024)
