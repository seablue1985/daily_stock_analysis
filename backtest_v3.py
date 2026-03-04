#!/usr/bin/env python3
"""
改进版小市值策略 v3 - 追求跑赢基准
目标：年化80%+，跑赢中证1000

改进点：
1. 扩大市值范围：20-80亿（更接近中证1000）
2. 加入动量因子：近20日涨幅
3. 更激进的选股：只看涨幅前50%
4. 增加持仓到20只
"""
import os
import pandas as pd
import numpy as np
from datetime import datetime

DATA_DIR = os.path.join(os.path.dirname(__file__), 'data')

def load_data():
    """加载数据"""
    basic = pd.read_csv(f'{DATA_DIR}/stock_basic.csv')
    daily = pd.read_csv(f'{DATA_DIR}/daily_basic_all.csv')
    return basic, daily

def calculate_momentum(daily_df, ts_code, date, days=20):
    """计算动量因子"""
    dates = sorted(daily_df['trade_date'].unique())
    
    # 找到目标日期的位置
    try:
        idx = dates.index(date)
        if idx < days:
            return 0
        
        # 获取过去N天的数据
        past_dates = dates[idx-days:idx]
        stock_data = daily_df[(daily_df['ts_code'] == ts_code) & 
                            (daily_df['trade_date'].isin(past_dates))]
        
        if len(stock_data) < days // 2:
            return 0
        
        # 计算涨幅
        first_price = stock_data.iloc[0]['close']
        last_price = stock_data.iloc[-1]['close']
        
        if first_price > 0:
            return (last_price - first_price) / first_price
    except:
        pass
    
    return 0

def select_stocks_v3(date=None, top_n=20, min_mv=20, max_mv=80):
    """
    v3版选股：市值适中+动量
    """
    basic, daily = load_data()
    
    # 最新日期
    if date is None:
        date = int(daily['trade_date'].max())
    
    df = daily[daily['trade_date'] == date].copy()
    
    # 转换市值单位
    df['circ_mv_yi'] = df['circ_mv'] / 10000
    
    # 1. 市值过滤：20-80亿
    df = df[(df['circ_mv_yi'] >= min_mv) & (df['circ_mv_yi'] <= max_mv)]
    
    # 2. PE过滤：排除亏损，PE<60
    df = df[(df['pe'] > 0) & (df['pe'] < 60)]
    
    # 3. 计算动量因子
    print(f"  计算动量因子...")
    mom_scores = []
    for _, row in df.iterrows():
        mom = calculate_momentum(daily, row['ts_code'], date, days=20)
        mom_scores.append(mom)
    df['momentum'] = mom_scores
    
    # 4. 动量过滤：排除近20日涨幅< -20%的（太弱）
    df = df[df['momentum'] > -0.20]
    
    # 5. 综合评分 = 市值因子 + 动量因子 + PE因子
    # 市值：越小分越高
    df['mv_score'] = (max_mv - df['circ_mv_yi']) / (max_mv - min_mv) * 10
    
    # 动量：涨幅越高分越高（但不要太高，防止追高）
    df['mom_score'] = np.clip(df['momentum'] * 10, 0, 5)
    
    # PE：合理区间分高
    df['pe_score'] = 0
    df.loc[(df['pe'] > 10) & (df['pe'] <= 30), 'pe_score'] = 3
    df.loc[(df['pe'] > 30) & (df['pe'] <= 50), 'pe_score'] = 2
    
    df['total_score'] = df['mv_score'] + df['mom_score'] + df['pe_score']
    
    # 6. 按评分排序
    df = df.sort_values('total_score', ascending=False)
    
    # 7. 合并名称
    df = df.merge(basic[['ts_code', 'name', 'industry']], on='ts_code', how='left')
    
    # 8. 行业分散
    result = df.sort_values('total_score', ascending=False).groupby('industry').head(2)
    result = result.sort_values('total_score', ascending=False).head(top_n)
    
    return result

def run_backtest_v3():
    """回测v3版策略"""
    print("📊 改进版小市值策略回测 v3")
    print("=" * 50)
    
    basic, daily = load_data()
    
    # 获取所有交易日
    dates = sorted(daily['trade_date'].unique())
    print(f"数据范围: {dates[0]} ~ {dates[-1]}")
    
    # 取最近120个交易日
    recent_dates = dates[-120:]
    
    # 按月回测
    monthly_returns = []
    
    months = sorted(set(d // 100 for d in recent_dates))
    
    for month in months:
        month_dates = [d for d in recent_dates if d // 100 == month]
        if not month_dates:
            continue
        
        trade_date = month_dates[0]
        
        # 选股
        selected = select_stocks_v3(date=trade_date, top_n=20, min_mv=20, max_mv=80)
        
        if len(selected) == 0:
            continue
        
        # 计算下月收益
        next_month_idx = months.index(month) + 1
        if next_month_idx < len(months):
            next_dates = [d for d in recent_dates if d // 100 == months[next_month_idx]]
            if next_dates:
                end_date = next_dates[-1]
                
                returns = []
                for _, stock in selected.iterrows():
                    stock_data = daily[(daily['ts_code'] == stock['ts_code']) & 
                                       (daily['trade_date'] == trade_date)]
                    stock_end = daily[(daily['ts_code'] == stock['ts_code']) & 
                                      (daily['trade_date'] == end_date)]
                    
                    if len(stock_data) > 0 and len(stock_end) > 0:
                        ret = (stock_end['close'].values[0] - stock_data['close'].values[0]) / stock_data['close'].values[0]
                        returns.append(ret)
                
                if returns:
                    avg_ret = np.mean(returns)
                    monthly_returns.append({
                        'date': trade_date,
                        'stocks': len(selected),
                        'return': avg_ret
                    })
    
    # 统计结果
    if monthly_returns:
        returns = [m['return'] for m in monthly_returns]
        total_return = np.prod([1 + r for r in returns]) - 1
        avg_return = np.mean(returns)
        win_rate = len([r for r in returns if r > 0]) / len(returns)
        
        n_years = len(returns) / 12
        annual_return = (1 + total_return) ** (1/n_years) - 1 if n_years > 0 else 0
        
        if np.std(returns) > 0:
            sharpe = (avg_return - 0.03/12) / np.std(returns) * np.sqrt(12)
        else:
            sharpe = 0
        
        # 最大回撤
        cumulative = np.cumprod([1 + r for r in returns])
        peak = np.maximum.accumulate(cumulative)
        drawdown = (cumulative - peak) / peak
        max_drawdown = drawdown.min()
        
        print(f"\n{'='*50}")
        print(f"v3版策略回测结果")
        print(f"{'='*50}")
        print(f"调仓次数: {len(returns)}")
        print(f"总收益率: {total_return*100:.2f}%")
        print(f"年化收益率: {annual_return*100:.2f}%")
        print(f"月均收益: {avg_return*100:.2f}%")
        print(f"胜率: {win_rate*100:.1f}%")
        print(f"夏普比率: {sharpe:.2f}")
        print(f"最大回撤: {max_drawdown*100:.2f}%")
        
        print(f"\n月度收益:")
        for m in monthly_returns:
            print(f"  {m['date']}: {m['return']*100:+.2f}%")
    
    return monthly_returns

if __name__ == '__main__':
    run_backtest_v3()
