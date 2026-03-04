#!/usr/bin/env python3
"""
小市值策略回测
基于历史数据的策略回测
"""
import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

DATA_DIR = os.path.join(os.path.dirname(__file__), 'data')

def load_all_data(years=None):
    """加载所有数据
    Args:
        years: 要加载的年份列表，如 [2021, 2022]。如果为None，则加载默认数据。
    """
    basic = pd.read_csv(f'{DATA_DIR}/stock_basic.csv')
    
    if years:
        # 加载指定年份的数据
        dfs = []
        for year in years:
            file_path = f'{DATA_DIR}/daily_{year}.csv'
            if os.path.exists(file_path):
                df = pd.read_csv(file_path)
                dfs.append(df)
                print(f"📊 加载 {year} 年数据: {len(df)} 条记录")
        
        if dfs:
            daily = pd.concat(dfs, ignore_index=True)
            print(f"📊 合计加载: {daily.ts_code.nunique()} 只股票, {len(daily)} 条记录")
        else:
            # 如果没有找到文件，回退到默认数据
            if os.path.exists(f'{DATA_DIR}/daily_basic_all.csv'):
                daily = pd.read_csv(f'{DATA_DIR}/daily_basic_all.csv')
                print(f"⚠️ 未找到历史数据文件，使用完整数据")
            else:
                daily = pd.read_csv(f'{DATA_DIR}/daily_basic_500.csv')
                print(f"⚠️ 使用样本数据: 500 只")
    else:
        # 优先使用完整数据
        if os.path.exists(f'{DATA_DIR}/daily_basic_all.csv'):
            daily = pd.read_csv(f'{DATA_DIR}/daily_basic_all.csv')
            print(f"📊 使用完整数据: {daily.ts_code.nunique()} 只股票")
        else:
            daily = pd.read_csv(f'{DATA_DIR}/daily_basic_500.csv')
            print(f"⚠️ 使用样本数据: 500 只")
    
    return basic, daily

def get_universe_by_date(date, daily_df):
    """获取指定日期的股票池"""
    df = daily_df[daily_df['trade_date'] == int(date)].copy()
    df['circ_mv_yi'] = df['circ_mv'] / 10000
    return df

def select_stocks(df, top_n=30, max_mv=50):
    """选股"""
    # 计算市值
    df = df.copy()
    df['circ_mv_yi'] = df['circ_mv'] / 10000
    
    # 过滤市值
    df = df[df['circ_mv_yi'] <= max_mv]
    df = df[df['circ_mv_yi'] > 0]
    
    # 过滤PE (排除亏损)
    df = df[df['pe'] > 0]
    df = df[df['pe'] < 80]
    
    # 按市值排序，取最小的
    df = df.sort_values('circ_mv_yi').head(top_n)
    
    return df

def calculate_returns(daily_df, stock_list, start_date, end_date):
    """计算持仓期收益"""
    # 获取时间范围内的所有日期
    dates = sorted(daily_df['trade_date'].unique())
    dates = [d for d in dates if start_date <= d <= end_date]
    
    if len(dates) < 2:
        return 0
    
    portfolio_value = 1.0  # 初始1元
    positions = {}  # 持仓
    
    # 每月第一个交易日调仓
    rebalance_months = set()
    for d in dates:
        month = d // 100
        rebalance_months.add(month)
    rebalance_months = sorted(rebalance_months)
    
    for i, month in enumerate(rebalance_months):
        # 获取该月的交易日
        month_dates = [d for d in dates if d // 100 == month]
        if not month_dates:
            continue
            
        trade_date = month_dates[0]
        
        # 获取当日小市值股票
        day_df = daily_df[daily_df['trade_date'] == trade_date]
        selected = select_stocks(day_df, top_n=30, max_mv=50)
        
        if len(selected) == 0:
            continue
        
        # 等权配置
        weight = 1.0 / len(selected)
        
        # 计算收益
        if i > 0 and positions:
            # 卖出上月持仓
            prev_month = rebalance_months[i-1]
            prev_dates = [d for d in dates if d // 100 == prev_month]
            if prev_dates:
                prev_date = prev_dates[-1]
                for stock in positions:
                    stock_data = daily_df[(daily_df['ts_code'] == stock) & (daily_df['trade_date'] == prev_date)]
                    if len(stock_data) > 0:
                        curr_price = stock_data['close'].values[0]
                        ret = (curr_price - positions[stock]['price']) / positions[stock]['price']
                        portfolio_value *= (1 + ret * weight * 30)  # 简化
        
        # 更新持仓
        for _, row in selected.iterrows():
            positions[row['ts_code']] = {'price': row['close']}
    
    return portfolio_value

def run_backtest(start_year=2021, end_year=2022):
    """运行回测
    Args:
        start_year: 起始年份
        end_year: 结束年份
    """
    print("📊 小市值策略回测")
    print("=" * 50)
    
    # 加载指定年份的数据
    years = list(range(start_year, end_year + 1))
    basic, daily = load_all_data(years)
    
    # 获取所有交易日
    dates = sorted(daily['trade_date'].unique())
    print(f"数据范围: {dates[0]} ~ {dates[-1]}")
    print(f"交易日数: {len(dates)}")
    
    # 选取指定年份的数据
    recent_dates = [d for d in dates if start_year <= d // 10000 <= end_year]
    if not recent_dates:
        # 如果没有指定年份的数据，使用最近120个交易日
        recent_dates = dates[-120:]
        print(f"⚠️ 未找到 {start_year}-{end_year} 年数据，使用最近120个交易日")
    
    start_date = min(recent_dates)
    end_date = max(recent_dates)
    
    print(f"\n回测区间: {start_date} ~ {end_date}")
    
    # 按月回测
    monthly_returns = []
    rebalance_points = []
    
    # 获取调仓月份
    months = sorted(set(d // 100 for d in recent_dates))
    
    for month in months:
        month_dates = [d for d in recent_dates if d // 100 == month]
        if not month_dates:
            continue
        
        trade_date = month_dates[0]
        
        # 选股
        day_df = daily[daily['trade_date'] == trade_date]
        selected = select_stocks(day_df, top_n=30, max_mv=50)
        
        if len(selected) == 0:
            continue
        
        # 合并名称
        selected = selected.merge(basic[['ts_code', 'name']], on='ts_code', how='left')
        
        # 计算下月收益
        if month_dates[-1] > recent_dates[0]:  # 有下月数据
            next_month = months[months.index(month) + 1] if months.index(month) + 1 < len(months) else None
            if next_month:
                next_dates = [d for d in recent_dates if d // 100 == next_month]
                if next_dates:
                    end_date = next_dates[-1]
                    
                    # 计算组合收益
                    returns = []
                    for _, stock in selected.iterrows():
                        stock_start = daily[(daily['ts_code'] == stock['ts_code']) & (daily['trade_date'] == trade_date)]
                        stock_end = daily[(daily['ts_code'] == stock['ts_code']) & (daily['trade_date'] == end_date)]
                        
                        if len(stock_start) > 0 and len(stock_end) > 0:
                            ret = (stock_end['close'].values[0] - stock_start['close'].values[0]) / stock_start['close'].values[0]
                            returns.append(ret)
                    
                    if returns:
                        avg_ret = np.mean(returns)
                        monthly_returns.append(avg_ret)
                        rebalance_points.append({
                            'date': trade_date,
                            'stocks': len(selected),
                            'return': avg_ret
                        })
    
    # 统计结果
    if monthly_returns:
        total_return = np.prod([1 + r for r in monthly_returns]) - 1
        avg_return = np.mean(monthly_returns)
        win_rate = len([r for r in monthly_returns if r > 0]) / len(monthly_returns)
        
        # 年化
        n_years = len(monthly_returns) / 12
        annual_return = (1 + total_return) ** (1/n_years) - 1 if n_years > 0 else 0
        
        # 夏普比率 (假设无风险利率3%)
        if np.std(monthly_returns) > 0:
            sharpe = (avg_return - 0.03/12) / np.std(monthly_returns) * np.sqrt(12)
        else:
            sharpe = 0
        
        print(f"\n{'='*50}")
        print(f"回测结果")
        print(f"{'='*50}")
        print(f"调仓次数: {len(monthly_returns)}")
        print(f"总收益率: {total_return*100:.2f}%")
        print(f"年化收益率: {annual_return*100:.2f}%")
        print(f"月均收益: {avg_return*100:.2f}%")
        print(f"胜率: {win_rate*100:.1f}%")
        print(f"夏普比率: {sharpe:.2f}")
        
        print(f"\n月度收益:")
        for i, r in enumerate(monthly_returns):
            print(f"  {rebalance_points[i]['date']}: {r*100:+.2f}%")
    
    return monthly_returns

if __name__ == '__main__':
    run_backtest()
