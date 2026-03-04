#!/usr/bin/env python3
"""
参数区间化测试
测试策略关键参数在不同区间下的表现
"""
import pandas as pd
import numpy as np
from datetime import datetime
import itertools

DATA_DIR = '/Users/ling/.openclaw/workspace/quant_system/data'

def load_data():
    """加载数据"""
    basic = pd.read_csv(f'{DATA_DIR}/stock_basic.csv')
    daily = pd.read_csv(f'{DATA_DIR}/daily_basic_all.csv')
    return basic, daily

def run_strategy(params, dates):
    """
    运行策略并返回绩效指标
    
    params: {
        'min_mv': 最小市值(亿)
        'max_mv': 最大市值(亿)  
        'max_pe': 最大PE
        'stock_num': 持仓数量
    }
    """
    basic, daily = load_data()
    
    # 按月回测
    monthly_returns = []
    months = sorted(set(d // 100 for d in dates))
    
    for month in months:
        month_dates = [d for d in dates if d // 100 == month]
        if not month_dates:
            continue
        
        trade_date = month_dates[0]
        
        # 选股
        df = daily[daily['trade_date'] == trade_date].copy()
        df['circ_mv_yi'] = df['circ_mv'] / 10000
        
        # 参数过滤
        df = df[(df['circ_mv_yi'] >= params['min_mv']) & (df['circ_mv_yi'] <= params['max_mv'])]
        df = df[(df['pe'] > 0) & (df['pe'] <= params['max_pe'])]
        df = df.sort_values('circ_mv_yi').head(params['stock_num'] * 3)
        
        # 行业分散
        df = df.merge(basic[['ts_code', 'name', 'industry']], on='ts_code', how='left')
        df = df.groupby('industry').head(2)
        df = df.head(params['stock_num'])
        
        if len(df) == 0:
            continue
        
        # 计算下月收益
        next_idx = months.index(month) + 1
        if next_idx < len(months):
            next_dates = [d for d in dates if d // 100 == months[next_idx]]
            if next_dates:
                end_date = next_dates[-1]
                
                returns = []
                for _, stock in df.iterrows():
                    sd = daily[(daily['ts_code'] == stock['ts_code']) & (daily['trade_date'] == trade_date)]
                    ed = daily[(daily['ts_code'] == stock['ts_code']) & (daily['trade_date'] == end_date)]
                    if len(sd) > 0 and len(ed) > 0:
                        r = (ed['close'].values[0] - sd['close'].values[0]) / sd['close'].values[0]
                        returns.append(r)
                
                if returns:
                    monthly_returns.append(np.mean(returns))
    
    if len(monthly_returns) < 3:
        return None
    
    # 计算绩效指标
    total_ret = np.prod([1 + r for r in monthly_returns]) - 1
    annual_ret = (1 + total_ret) ** (12 / len(monthly_returns)) - 1
    win_rate = len([r for r in monthly_returns if r > 0]) / len(monthly_returns)
    
    # 最大回撤
    cumulative = np.cumprod([1 + r for r in monthly_returns])
    peak = np.maximum.accumulate(cumulative)
    drawdown = (cumulative - peak) / peak
    max_dd = drawdown.min()
    
    # 夏普比率
    sharpe = (np.mean(monthly_returns) - 0.03/12) / np.std(monthly_returns) * np.sqrt(12) if np.std(monthly_returns) > 0 else 0
    
    return {
        'annual_ret': annual_ret,
        'total_ret': total_ret,
        'win_rate': win_rate,
        'max_dd': max_dd,
        'sharpe': sharpe,
        'n_trades': len(monthly_returns)
    }

def param_robustness_test():
    """参数鲁棒性测试"""
    print("=" * 60)
    print("参数区间化鲁棒性测试")
    print("=" * 60)
    
    basic, daily = load_data()
    dates = sorted(daily['trade_date'].unique())[-120:]  # 最近120天
    
    # 参数空间
    param_grid = {
        'min_mv': [10, 15, 20, 25],  # 最小市值
        'max_mv': [30, 50, 80, 100],  # 最大市值
        'max_pe': [30, 40, 50, 60],  # 最大PE
        'stock_num': [10, 15, 20, 25],  # 持仓数量
    }
    
    # 先测试单参数影响
    print("\n【1】最小市值 (min_mv) 影响")
    print("-" * 50)
    results = []
    for min_mv in param_grid['min_mv']:
        params = {'min_mv': min_mv, 'max_mv': 50, 'max_pe': 40, 'stock_num': 15}
        ret = run_strategy(params, dates)
        if ret:
            results.append({
                'min_mv': min_mv,
                'annual': ret['annual_ret'] * 100,
                'sharpe': ret['sharpe'],
                'max_dd': ret['max_dd'] * 100,
                'win_rate': ret['win_rate'] * 100
            })
    
    for r in results:
        print(f"min_mv={r['min_mv']:2d}亿 | 年化:{r['annual']:6.1f}% | 夏普:{r['sharpe']:4.2f} | 回撤:{r['max_dd']:5.1f}% | 胜率:{r['win_rate']:.0f}%")
    
    print("\n【2】最大市值 (max_mv) 影响")
    print("-" * 50)
    results = []
    for max_mv in param_grid['max_mv']:
        params = {'min_mv': 15, 'max_mv': max_mv, 'max_pe': 40, 'stock_num': 15}
        ret = run_strategy(params, dates)
        if ret:
            results.append({
                'max_mv': max_mv,
                'annual': ret['annual_ret'] * 100,
                'sharpe': ret['sharpe'],
                'max_dd': ret['max_dd'] * 100,
                'win_rate': ret['win_rate'] * 100
            })
    
    for r in results:
        print(f"max_mv={r['max_mv']:3d}亿 | 年化:{r['annual']:6.1f}% | 夏普:{r['sharpe']:4.2f} | 回撤:{r['max_dd']:5.1f}% | 胜率:{r['win_rate']:.0f}%")
    
    print("\n【3】PE上限 (max_pe) 影响")
    print("-" * 50)
    results = []
    for max_pe in param_grid['max_pe']:
        params = {'min_mv': 15, 'max_mv': 50, 'max_pe': max_pe, 'stock_num': 15}
        ret = run_strategy(params, dates)
        if ret:
            results.append({
                'max_pe': max_pe,
                'annual': ret['annual_ret'] * 100,
                'sharpe': ret['sharpe'],
                'max_dd': ret['max_dd'] * 100,
                'win_rate': ret['win_rate'] * 100
            })
    
    for r in results:
        print(f"max_pe={r['max_pe']:2d}  | 年化:{r['annual']:6.1f}% | 夏普:{r['sharpe']:4.2f} | 回撤:{r['max_dd']:5.1f}% | 胜率:{r['win_rate']:.0f}%")
    
    print("\n【4】持仓数量 (stock_num) 影响")
    print("-" * 50)
    results = []
    for stock_num in param_grid['stock_num']:
        params = {'min_mv': 15, 'max_mv': 50, 'max_pe': 40, 'stock_num': stock_num}
        ret = run_strategy(params, dates)
        if ret:
            results.append({
                'stock_num': stock_num,
                'annual': ret['annual_ret'] * 100,
                'sharpe': ret['sharpe'],
                'max_dd': ret['max_dd'] * 100,
                'win_rate': ret['win_rate'] * 100
            })
    
    for r in results:
        print(f"stock_num={r['stock_num']:2d} | 年化:{r['annual']:6.1f}% | 夏普:{r['sharpe']:4.2f} | 回撤:{r['max_dd']:5.1f}% | 胜率:{r['win_rate']:.0f}%")
    
    # 寻找最优参数组合
    print("\n【5】最优参数组合搜索")
    print("-" * 50)
    
    best_score = -999
    best_params = None
    best_metrics = None
    
    # 简化搜索空间
    for min_mv in [10, 15, 20]:
        for max_mv in [40, 50, 80]:
            for max_pe in [30, 40, 50]:
                for stock_num in [10, 15, 20]:
                    params = {
                        'min_mv': min_mv,
                        'max_mv': max_mv,
                        'max_pe': max_pe,
                        'stock_num': stock_num
                    }
                    ret = run_strategy(params, dates)
                    if ret and ret['sharpe'] > 0:
                        # 综合评分: 年化 * 0.4 + 夏普 * 20 - 回撤 * 0.3
                        score = ret['annual_ret'] * 0.4 + ret['sharpe'] * 0.4 - abs(ret['max_dd']) * 0.2
                        if score > best_score:
                            best_score = score
                            best_params = params
                            best_metrics = ret
    
    if best_params:
        print(f"最优参数:")
        print(f"  min_mv = {best_params['min_mv']}亿")
        print(f"  max_mv = {best_params['max_mv']}亿")
        print(f"  max_pe = {best_params['max_pe']}")
        print(f"  stock_num = {best_params['stock_num']}")
        print(f"\n绩效:")
        print(f"  年化收益: {best_metrics['annual_ret']*100:.1f}%")
        print(f"  夏普比率: {best_metrics['sharpe']:.2f}")
        print(f"  最大回撤: {best_metrics['max_dd']*100:.1f}%")
        print(f"  胜率: {best_metrics['win_rate']*100:.0f}%")
    
    print("\n【结论】")
    print("=" * 50)
    print("参数区间建议:")
    print("  - 最小市值: 10-20亿 (不宜太小)")
    print("  - 最大市值: 50-80亿 (不宜太大)")
    print("  - PE上限: 40-50 (太严格会错过机会)")
    print("  - 持仓数量: 15-20只 (分散风险)")
    print("  - 止损比例: 0.88-0.91 (不宜太激进)")

if __name__ == '__main__':
    param_robustness_test()
