#!/usr/bin/env python3
"""
国九小市值策略 - 基于聚宽社区zycash的策略逻辑
核心:
1. 小市值选股 (<1亿)
2. 国九条筛选 (净利润>0)
3. 每周二调仓
4. 4月和1月空仓
5. 10%止损
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
    daily = pd.concat(dfs, ignore_index=True) if dfs else None
    return daily


def run_strategy(start_year=2021, end_year=2024):
    """运行国九小市值策略"""
    print("="*60)
    print("📊 国九小市值策略 (聚宽 zycash 版)")
    print("="*60)
    
    # 参数
    MAX_MV = 1e8  # 最大市值1亿
    TOP_N = 25    # 持仓25只
    STOP_LOSS = -0.10  # 止损10%
    
    years = list(range(start_year, end_year + 1))
    daily = load_data(years)
    
    if daily is None:
        print("❌ 没有数据")
        return
    
    dates = sorted(daily['trade_date'].unique())
    print(f"数据范围: {dates[0]} ~ {dates[-1]}")
    
    # 按周提取交易日 (每周二)
    dates_df = pd.DataFrame({'trade_date': dates})
    dates_df['year'] = dates_df['trade_date'] // 10000
    dates_df['month'] = (dates_df['trade_date'] // 100) % 100
    dates_df['weekday'] = pd.to_datetime(dates_df['trade_date'].astype(str)).dt.weekday
    
    # 每周二调仓
    trade_dates = dates_df[dates_df['weekday'] == 1]['trade_date'].tolist()
    
    # 过滤空仓月份 (4月和1月)
    active_dates = []
    for d in trade_dates:
        month = (d // 100) % 100
        if month not in [1, 4]:  # 1月和4月空仓
            active_dates.append(d)
    
    print(f"调仓日数量: {len(active_dates)}")
    
    # 选股函数
    def select_stocks(df, top_n=25):
        df = df.copy()
        df['circ_mv_yi'] = df['circ_mv'] / 10000  # 转换为亿
        # 小市值筛选
        df = df[df['circ_mv_yi'] <= 100]  # <100亿
        df = df[df['circ_mv_yi'] > 0]
        # PE筛选
        df = df[df['pe'] > 0]
        df = df[df['pe'] < 100]
        # 净利润>0 (如果有该字段)
        if 'net_profit' in df.columns:
            df = df[df['net_profit'] > 0]
        # 按市值升序
        return df.sort_values('circ_mv_yi').head(top_n)
    
    # 回测
    monthly_returns = []
    prev_positions = {}
    
    for i, trade_date in enumerate(active_dates):
        day_df = daily[daily['trade_date'] == trade_date]
        if len(day_df) == 0:
            continue
        
        # 选股
        selected = select_stocks(day_df, TOP_N)
        
        if len(selected) == 0:
            continue
        
        # 计算收益
        if i + 1 < len(active_dates):
            next_date = active_dates[i + 1]
            
            stock_returns = []
            for ts_code in selected['ts_code'].values:
                # 止损检查
                if ts_code in prev_positions:
                    stock_start = day_df[day_df['ts_code'] == ts_code]
                    if len(stock_start) > 0:
                        current_price = stock_start['close'].values[0]
                        ret = (current_price - prev_positions[ts_code]) / prev_positions[ts_code]
                        if ret < STOP_LOSS:
                            continue
                
                stock_start = day_df[day_df['ts_code'] == ts_code]
                stock_end = daily[(daily['ts_code'] == ts_code) & (daily['trade_date'] == next_date)]
                
                if len(stock_start) > 0 and len(stock_end) > 0:
                    ret = (stock_end['close'].values[0] - stock_start['close'].values[0]) / stock_start['close'].values[0]
                    stock_returns.append(ret)
            
            if stock_returns:
                avg_ret = np.mean(stock_returns)
                monthly_returns.append({'date': trade_date, 'return': avg_ret})
            
            # 更新持仓
            prev_positions = {}
            for _, row in selected.iterrows():
                prev_positions[row['ts_code']] = row['close']
    
    # 统计结果
    if not monthly_returns:
        print("❌ 没有收益数据")
        return
    
    returns = [r['return'] for r in monthly_returns]
    total_return = np.prod([1 + r for r in returns]) - 1
    n = len(returns)
    annual_return = (1 + total_return) ** (12 / n) - 1
    
    vol = np.std(returns)
    annual_vol = vol * np.sqrt(52)  # 周频率
    sharpe = (annual_return - 0.03) / annual_vol if annual_vol > 0 else 0
    
    # 最大回撤
    cumprod = np.cumprod([1 + r for r in returns])
    peak = np.maximum.accumulate(cumprod)
    drawdown = (cumprod - peak) / peak
    max_dd = np.min(drawdown)
    
    win_rate = len([r for r in returns if r > 0]) / n
    
    print(f"\n📈 回测结果")
    print(f"{'='*50}")
    print(f"调仓次数: {n}")
    print(f"总收益率: {total_return*100:.2f}%")
    print(f"年化收益率: {annual_return*100:.2f}%")
    print(f"年化波动率: {annual_vol*100:.2f}%")
    print(f"夏普比率: {sharpe:.2f}")
    print(f"最大回撤: {max_dd*100:.2f}%")
    print(f"胜率: {win_rate*100:.1f}%")
    
    print(f"\n📉 最近收益:")
    for r in monthly_returns[-10:]:
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
