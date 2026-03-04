#!/usr/bin/env python3
"""
小市值策略回测 - 带风控版本
"""
import os
import pandas as pd
import numpy as np

DATA_DIR = os.path.join(os.path.dirname(__file__), 'data')

# ==================== 风控参数 ====================
STOP_LOSS = -0.10    # 单股止损线 -10%
MAX_POSITION = 0.05  # 单股最大仓位 5%
PE_MIN = 0           # 最小PE
PE_MAX = 80          # 最大PE


def load_all_data(years=None):
    """加载数据"""
    basic = pd.read_csv(f'{DATA_DIR}/stock_basic.csv')
    
    if years:
        dfs = []
        for year in years:
            path = f'{DATA_DIR}/daily_{year}.csv'
            if os.path.exists(path):
                df = pd.read_csv(path)
                dfs.append(df)
                print(f"📊 加载 {year} 年数据: {len(df)} 条记录")
        
        if dfs:
            daily = pd.concat(dfs, ignore_index=True)
            print(f"📊 合计: {daily.ts_code.nunique()} 只股票, {len(daily)} 条记录")
        else:
            if os.path.exists(f'{DATA_DIR}/daily_basic_all.csv'):
                daily = pd.read_csv(f'{DATA_DIR}/daily_basic_all.csv')
            else:
                daily = pd.read_csv(f'{DATA_DIR}/daily_basic_500.csv')
    else:
        if os.path.exists(f'{DATA_DIR}/daily_basic_all.csv'):
            daily = pd.read_csv(f'{DATA_DIR}/daily_basic_all.csv')
        else:
            daily = pd.read_csv(f'{DATA_DIR}/daily_basic_500.csv')
    
    return basic, daily


def select_stocks(df, top_n=30, max_mv=50):
    """选股"""
    df = df.copy()
    df['circ_mv_yi'] = df['circ_mv'] / 10000
    df = df[(df['circ_mv_yi'] <= max_mv) & (df['circ_mv_yi'] > 0)]
    df = df[(df['pe'] > PE_MIN) & (df['pe'] < PE_MAX)]
    return df.sort_values('circ_mv_yi').head(top_n)


def run_backtest(start_year=2021, end_year=2024, top_n=30, max_mv=50, 
                 use_stop_loss=False, use_position_limit=False):
    """运行回测
    
    Args:
        start_year: 起始年
        end_year: 结束年
        top_n: 持仓数量
        max_mv: 最大市值(亿)
        use_stop_loss: 是否使用止损
        use_position_limit: 是否使用仓位限制
    """
    print("="*60)
    print(f"📊 小市值策略回测 ({start_year}-{end_year})")
    if use_stop_loss:
        print("🛡️ 止损: -10%")
    if use_position_limit:
        print("🛡️ 仓位: 单股最多5%")
    print("="*60)
    
    years = list(range(start_year, end_year + 1))
    basic, daily = load_all_data(years)
    
    dates = sorted(daily['trade_date'].unique())
    print(f"数据范围: {dates[0]} ~ {dates[-1]}")
    print(f"交易日数: {len(dates)}")
    
    months = sorted(set(d // 100 for d in dates))
    
    monthly_returns = []
    rebalance_dates = []
    
    # 记录持仓用于止损检查
    prev_positions = {}
    
    for i, month in enumerate(months):
        month_dates = [d for d in dates if d // 100 == month]
        if not month_dates:
            continue
        
        trade_date = month_dates[0]
        
        # 选股
        day_df = daily[daily['trade_date'] == trade_date]
        selected = select_stocks(day_df, top_n, max_mv)
        
        if len(selected) == 0:
            continue
        
        # 获取下月数据计算收益
        if i + 1 < len(months):
            next_month = months[i + 1]
            next_dates = [d for d in dates if d // 100 == next_month]
            if next_dates:
                end_date = next_dates[-1]
                
                # 计算每只股票的收益
                stock_returns = []
                selected_codes = selected['ts_code'].values
                
                for ts_code in selected_codes:
                    # 持仓检查
                    if use_stop_loss and ts_code in prev_positions:
                        prev_buy_price = prev_positions[ts_code]
                        stock_start = daily[(daily['ts_code'] == ts_code) & (daily['trade_date'] == trade_date)]
                        if len(stock_start) > 0:
                            current_price = stock_start['close'].values[0]
                            ret = (current_price - prev_buy_price) / prev_buy_price
                            if ret < STOP_LOSS:
                                # 止损，不计入收益
                                continue
                    
                    # 计算收益
                    stock_start = daily[(daily['ts_code'] == ts_code) & (daily['trade_date'] == trade_date)]
                    stock_end = daily[(daily['ts_code'] == ts_code) & (daily['trade_date'] == end_date)]
                    
                    if len(stock_start) > 0 and len(stock_end) > 0:
                        ret = (stock_end['close'].values[0] - stock_start['close'].values[0]) / stock_start['close'].values[0]
                        
                        # 止损检查 (如果用持仓价格计算)
                        if use_stop_loss and ts_code in prev_positions:
                            if ret < STOP_LOSS:
                                continue
                        
                        stock_returns.append(ret)
                
                if stock_returns:
                    avg_return = np.mean(stock_returns)
                    monthly_returns.append(avg_return)
                    rebalance_dates.append({'date': trade_date, 'return': avg_return})
                
                # 更新持仓记录
                if use_stop_loss:
                    prev_positions = {}
                    for ts_code in selected_codes:
                        stock_data = daily[(daily['ts_code'] == ts_code) & (daily['trade_date'] == trade_date)]
                        if len(stock_data) > 0:
                            prev_positions[ts_code] = stock_data['close'].values[0]
    
    # ==================== 统计结果 ====================
    if not monthly_returns:
        print("❌ 没有收益数据")
        return
    
    total_return = np.prod([1 + r for r in monthly_returns]) - 1
    n = len(monthly_returns)
    annual_return = (1 + total_return) ** (12 / n) - 1
    
    monthly_vol = np.std(monthly_returns)
    annual_vol = monthly_vol * np.sqrt(12)
    sharpe = (annual_return - 0.03) / annual_vol if annual_vol > 0 else 0
    
    # 最大回撤
    cumprod = np.cumprod([1 + r for r in monthly_returns])
    peak = np.maximum.accumulate(cumprod)
    drawdown = (cumprod - peak) / peak
    max_dd = np.min(drawdown)
    
    win_rate = len([r for r in monthly_returns if r > 0]) / n
    
    print(f"\n📈 回测结果")
    print(f"{'='*50}")
    print(f"调仓次数: {n}")
    print(f"总收益率: {total_return*100:.2f}%")
    print(f"年化收益率: {annual_return*100:.2f}%")
    print(f"年化波动率: {annual_vol*100:.2f}%")
    print(f"夏普比率: {sharpe:.2f}")
    print(f"最大回撤: {max_dd*100:.2f}%")
    print(f"胜率: {win_rate*100:.1f}%")
    
    # 年度表现
    df = pd.DataFrame({'month': months[:len(monthly_returns)], 'return': monthly_returns})
    df['year'] = df['month'] // 100
    yearly = df.groupby('year')['return'].apply(lambda x: np.prod(1+x) - 1)
    
    print(f"\n📅 年度表现:")
    for year, ret in yearly.items():
        print(f"  {year}: {ret*100:+.2f}%")
    
    print(f"\n📉 月度收益:")
    for j, (d, r) in enumerate(zip(rebalance_dates[:12], monthly_returns[:12])):
        print(f"  {d['date']}: {r*100:+.2f}%")
    
    return {
        'total_return': total_return,
        'annual_return': annual_return,
        'sharpe': sharpe,
        'max_drawdown': max_dd,
        'win_rate': win_rate,
        'monthly_returns': monthly_returns
    }


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--start', type=int, default=2021)
    parser.add_argument('--end', type=int, default=2024)
    parser.add_argument('--top-n', type=int, default=30)
    parser.add_argument('--max-mv', type=int, default=50)
    parser.add_argument('--stop-loss', action='store_true')
    parser.add_argument('--position-limit', action='store_true')
    args = parser.parse_args()
    
    run_backtest(args.start, args.end, args.top_n, args.max_mv,
                 args.stop_loss, args.position_limit)
