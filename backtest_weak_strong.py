#!/usr/bin/env python3
"""
国证2000弱转强策略
基于聚宽社区"国证2000弱转强"策略改编
"""
import os
import pandas as pd
import numpy as np
import akshare as ak
from datetime import datetime, timedelta

DATA_DIR = os.path.join(os.path.dirname(__file__), 'data')


def load_data(years):
    """加载数据"""
    dfs = []
    for year in years:
        path = f'{DATA_DIR}/daily_{year}.csv'
        if os.path.exists(path):
            dfs.append(pd.read_csv(path))
    return pd.concat(dfs, ignore_index=True) if dfs else None


def get_index_stocks():
    """获取国证2000成分股"""
    # 尝试从本地数据获取
    index_file = f'{DATA_DIR}/index_000852.csv'
    if os.path.exists(index_file):
        df = pd.read_csv(index_file)
        if 'ts_code' in df.columns:
            return df['ts_code'].tolist()
    return None


def run_strategy(start_year=2021, end_year=2024):
    """运行弱转强策略"""
    print("="*60)
    print("📊 国证2000弱转强策略")
    print("="*60)
    
    # 参数
    STOCK_NUM = 8  # 持仓数量
    STOP_LOSS = -0.10  # 止损10%
    
    years = list(range(start_year, end_year + 1))
    daily = load_data(years)
    
    if daily is None:
        print("❌ 没有数据")
        return
    
    dates = sorted(daily['trade_date'].unique())
    print(f"数据范围: {dates[0]} ~ {dates[-1]}")
    
    # 获取成分股
    index_stocks = get_index_stocks()
    if index_stocks:
        print(f"使用国证2000成分股: {len(index_stocks)} 只")
    else:
        # 用全部股票
        index_stocks = daily['ts_code'].unique().tolist()
        print(f"使用全部股票: {len(index_stocks)} 只")
    
    # 按日回测
    daily_returns = []
    prev_positions = {}
    positions = {}  # 当前持仓
    
    for i, trade_date in enumerate(dates):
        # 排除1月、4月、12月
        month = (trade_date // 100) % 100
        if month in [1, 4, 12]:
            continue
        
        # 获取当日数据
        day_df = daily[daily['trade_date'] == trade_date]
        if len(day_df) == 0:
            continue
        
        # 筛选条件
        # 1. 小市值
        day_df = day_df.copy()
        day_df['circ_mv_yi'] = day_df['circ_mv'] / 10000
        day_df = day_df[day_df['circ_mv_yi'] <= 100]
        day_df = day_df[day_df['circ_mv_yi'] > 0]
        
        # 2. PE > 0 (盈利筛选)
        day_df = day_df[day_df['pe'] > 0]
        day_df = day_df[day_df['pe'] < 80]
        
        # 3. 过滤ST
        day_df = day_df[~day_df['ts_code'].str.contains('ST|ST')]
        
        if len(day_df) == 0:
            continue
        
        # 按市值升序排序，选最小的
        selected = day_df.sort_values('circ_mv_yi').head(STOCK_NUM * 3)
        
        # 计算收益
        if i + 1 < len(dates):
            next_date = dates[i + 1]
            next_df = daily[daily['trade_date'] == next_date]
            
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
                stock_end = next_df[next_df['ts_code'] == ts_code]
                
                if len(stock_start) > 0 and len(stock_end) > 0:
                    ret = (stock_end['close'].values[0] - stock_start['close'].values[0]) / stock_start['close'].values[0]
                    stock_returns.append(ret)
            
            if stock_returns:
                avg_ret = np.mean(stock_returns)
                daily_returns.append({
                    'date': trade_date,
                    'return': avg_ret,
                    'count': len(stock_returns)
                })
            
            # 更新持仓
            prev_positions = {}
            for _, row in selected.head(STOCK_NUM).iterrows():
                prev_positions[row['ts_code']] = row['close']
    
    # 统计结果
    if not daily_returns:
        print("❌ 没有收益数据")
        return
    
    returns = [r['return'] for r in daily_returns]
    total_return = np.prod([1 + r for r in returns]) - 1
    n = len(returns)
    annual_return = (1 + total_return) ** (250 / n) - 1
    
    vol = np.std(returns)
    annual_vol = vol * np.sqrt(250)
    sharpe = (annual_return - 0.03) / annual_vol if annual_vol > 0 else 0
    
    # 最大回撤
    cumprod = np.cumprod([1 + r for r in returns])
    peak = np.maximum.accumulate(cumprod)
    drawdown = (cumprod - peak) / peak
    max_dd = np.min(drawdown)
    
    win_rate = len([r for r in returns if r > 0]) / n
    
    print(f"\n📈 回测结果")
    print(f"{'='*50}")
    print(f"交易天数: {n}")
    print(f"总收益率: {total_return*100:.2f}%")
    print(f"年化收益率: {annual_return*100:.2f}%")
    print(f"年化波动率: {annual_vol*100:.2f}%")
    print(f"夏普比率: {sharpe:.2f}")
    print(f"最大回撤: {max_dd*100:.2f}%")
    print(f"胜率: {win_rate*100:.1f}%")
    
    print(f"\n📉 最近收益:")
    for r in daily_returns[-10:]:
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
