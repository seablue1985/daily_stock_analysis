#!/usr/bin/env python3
"""
小市值策略优化版 v2 - 加入止损、仓位管理、风控
"""
import os
import pandas as pd
import numpy as np

DATA_DIR = os.path.join(os.path.dirname(__file__), 'data')


def load_data(years):
    """加载数据"""
    basic = pd.read_csv(f'{DATA_DIR}/stock_basic.csv')
    dfs = []
    for year in years:
        path = f'{DATA_DIR}/daily_{year}.csv'
        if os.path.exists(path):
            dfs.append(pd.read_csv(path))
    daily = pd.concat(dfs, ignore_index=True) if dfs else None
    return basic, daily


def select_stocks(df, top_n=30, max_mv=50):
    """选股"""
    df = df.copy()
    df['circ_mv_yi'] = df['circ_mv'] / 10000
    df = df[(df['circ_mv_yi'] <= max_mv) & (df['circ_mv_yi'] > 0)]
    df = df[(df['pe'] > 0) & (df['pe'] < 80)]
    return df.sort_values('circ_mv_yi').head(top_n)


def run_backtest_v2(start_year=2021, end_year=2024):
    """运行优化版回测 v2"""
    print("="*60)
    print("📊 小市值策略优化版 v2")
    print("="*60)
    
    # 参数
    TOP_N = 30
    MAX_MV = 50
    STOP_LOSS = -0.10  # 止损 -10%
    MAX_POSITION = 0.05  # 单股最大5%
    BASE_POSITION = 0.90  # 基础仓位90%
    
    years = list(range(start_year, end_year + 1))
    basic, daily = load_data(years)
    
    dates = sorted(daily['trade_date'].unique())
    print(f"数据范围: {dates[0]} ~ {dates[-1]}")
    
    months = sorted(set(d // 100 for d in dates))
    
    # 初始资金
    cash = 1000000
    positions = {}  # {ts_code: {'shares': x, 'buy_price': y}}
    peak_value = cash
    
    monthly_returns = []
    drawdown_alerts = []
    
    for i, month in enumerate(months):
        month_dates = [d for d in dates if d // 100 == month]
        if not month_dates:
            continue
        
        trade_date = month_dates[0]
        
        # 计算当前持仓市值
        position_value = 0
        positions_to_remove = []
        
        for ts_code, pos in positions.items():
            stock_day = daily[(daily['ts_code'] == ts_code) & (daily['trade_date'] == trade_date)]
            if len(stock_day) > 0:
                current_price = stock_day['close'].values[0]
                pos['current_price'] = current_price
                position_value += pos['shares'] * current_price
                
                # 止损检查
                ret = (current_price - pos['buy_price']) / pos['buy_price']
                if ret < STOP_LOSS:
                    # 止损卖出
                    cash += pos['shares'] * current_price
                    positions_to_remove.append(ts_code)
        
        # 移除止损的股票
        for ts_code in positions_to_remove:
            del positions[ts_code]
        
        total_value = cash + position_value
        
        # 回撤检查 - 动态调整仓位
        if total_value > peak_value:
            peak_value = total_value
        
        drawdown = (total_value - peak_value) / peak_value
        
        # 根据回撤调整仓位
        if drawdown < -0.20:
            target_position_ratio = 0.50
            action = "强减仓50%"
        elif drawdown < -0.15:
            target_position_ratio = 0.70
            action = "减仓70%"
        elif drawdown < -0.10:
            target_position_ratio = 0.80
            action = "微减仓80%"
        else:
            target_position_ratio = BASE_POSITION
        
        if drawdown < -0.10 and (not drawdown_alerts or drawdown_alerts[-1]['month'] != month):
            drawdown_alerts.append({'month': month, 'action': action, 'drawdown': drawdown})
        
        # 选股
        day_df = daily[daily['trade_date'] == trade_date]
        selected = select_stocks(day_df, TOP_N, MAX_MV)
        
        if len(selected) == 0:
            continue
        
        # 计算目标持仓
        target_value = total_value * target_position_ratio
        target_per_stock = target_value * MAX_POSITION
        
        # 需要买入的股票
        current_holdings = set(positions.keys())
        target_holdings = set(selected['ts_code'].values)
        
        # 卖出不再持有的
        for ts_code in list(positions.keys()):
            if ts_code not in target_holdings:
                pos = positions[ts_code]
                cash += pos['shares'] * pos['current_price']
                del positions[ts_code]
        
        # 买入新股票
        for _, stock in selected.iterrows():
            ts_code = stock['ts_code']
            if ts_code not in positions:
                buy_price = stock['close']
                if cash > target_per_stock:
                    shares = int(target_per_stock / buy_price)
                    if shares > 0:
                        cost = shares * buy_price
                        cash -= cost
                        positions[ts_code] = {
                            'shares': shares,
                            'buy_price': buy_price,
                            'current_price': buy_price
                        }
        
        # 计算月末收益
        end_date = month_dates[-1]
        month_end_value = 0
        
        for ts_code, pos in list(positions.items()):
            stock_end = daily[(daily['ts_code'] == ts_code) & (daily['trade_date'] == end_date)]
            if len(stock_end) > 0:
                end_price = stock_end['close'].values[0]
                month_end_value += pos['shares'] * end_price
                pos['current_price'] = end_price
            else:
                month_end_value += pos['shares'] * pos['current_price']
        
        month_total = cash + month_end_value
        month_return = (month_total - total_value) / total_value
        monthly_returns.append({'month': month, 'return': month_return, 'value': month_total})
        
        cash = month_total
    
    # ==================== 统计结果 ====================
    returns = [r['return'] for r in monthly_returns]
    df = pd.DataFrame(monthly_returns)
    df['cumprod'] = (1 + df['return']).cumprod()
    df['peak'] = df['cumprod'].cummax()
    df['drawdown'] = (df['cumprod'] - df['peak']) / df['peak']
    
    total_return = df['cumprod'].iloc[-1] - 1
    n_months = len(returns)
    annual_return = (1 + total_return) ** (12 / n_months) - 1
    
    monthly_vol = np.std(returns)
    annual_vol = monthly_vol * np.sqrt(12)
    sharpe = (annual_return - 0.03) / annual_vol if annual_vol > 0 else 0
    
    max_dd = df['drawdown'].min()
    win_rate = len([r for r in returns if r > 0]) / len(returns)
    
    # 年度表现
    df['year'] = df['month'] // 100
    yearly = df.groupby('year')['return'].apply(lambda x: (1+x).prod()-1)
    
    print(f"\n📈 回测结果")
    print(f"{'='*50}")
    print(f"调仓次数: {n_months}")
    print(f"总收益率: {total_return*100:.2f}%")
    print(f"年化收益率: {annual_return*100:.2f}%")
    print(f"年化波动率: {annual_vol*100:.2f}%")
    print(f"夏普比率: {sharpe:.2f}")
    print(f"最大回撤: {max_dd*100:.2f}%")
    print(f"胜率: {win_rate*100:.1f}%")
    
    print(f"\n📅 年度表现:")
    for year, ret in yearly.items():
        print(f"  {year}: {ret*100:+.2f}%")
    
    print(f"\n📉 月度收益 (前12个月):")
    for j, row in df.head(12).iterrows():
        print(f"  {int(row['month'])}: {row['return']*100:+.2f}%")
    
    if drawdown_alerts:
        print(f"\n⚠️ 风控操作:")
        for a in drawdown_alerts[:5]:
            print(f"  {a['month']}: {a['action']} (回撤{a['drawdown']*100:.1f}%)")
    
    return df


if __name__ == '__main__':
    run_backtest_v2(2021, 2024)
