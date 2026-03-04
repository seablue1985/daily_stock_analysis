#!/usr/bin/env python3
"""
5年完整回测 + Alpha/Beta计算
"""
import pandas as pd
import numpy as np

DATA_DIR = '/Users/ling/.openclaw/workspace/quant_system/data'

def load_index_data():
    """加载指数数据"""
    indices = {
        'hs300': '000300_SH.csv',  # 沪深300
        'zz500': '000905_SH.csv',  # 中证500
        'zz1000': '000852_SH.csv', # 中证1000
    }
    
    data = {}
    for name, filename in indices.items():
        df = pd.read_csv(f'{DATA_DIR}/index_{filename}')
        df['date'] = pd.to_datetime(df['trade_date'], format='%Y%m%d')
        df = df.sort_values('date')
        df['return'] = df['close'].pct_change()  # 日收益率
        data[name] = df
    
    return data

def strategy_etf_rotation(data):
    """ETF轮动策略"""
    print("\n" + "="*60)
    print("【策略】ETF轮动策略 (2019-2026)")
    print("="*60)
    
    # 获取共同日期
    dates = data['hs300']['date'].tolist()
    
    # 按月调仓
    monthly_returns = []
    benchmark_returns = []
    
    for i in range(20, len(dates), 20):  # 每20天
        current_date = dates[i]
        
        # 计算过去20天动量
        momentum = {}
        for name, df in data.items():
            df_period = df[(df['date'] < current_date) & (df['date'] >= dates[i-20])]
            if len(df_period) >= 10:
                ret = (df_period['close'].iloc[-1] / df_period['close'].iloc[0]) - 1
                momentum[name] = ret
        
        if not momentum:
            continue
        
        # 选最强动量
        best = max(momentum, key=momentum.get)
        
        # 下20天收益
        if i + 20 < len(dates):
            next_date = dates[i + 20]
            
            for name, df in data.items():
                start = df[df['date'] == current_date]['close'].values
                end = df[df['date'] == next_date]['close'].values
                
                if len(start) > 0 and len(end) > 0:
                    ret = (end[0] / start[0]) - 1
                    if name == best:
                        monthly_returns.append(ret)
                    if name == 'zz1000':  # 中证1000作为基准
                        benchmark_returns.append(ret)
    
    return monthly_returns, benchmark_returns

def calculate_metrics(returns, benchmark_returns):
    """计算绩效指标"""
    # 转换为numpy数组
    returns = np.array(returns)
    benchmark_returns = np.array(benchmark_returns)
    
    # 基本指标
    total_return = np.prod(1 + returns) - 1
    n_years = len(returns) / 12
    annual_return = (1 + total_return) ** (1/n_years) - 1 if n_years > 0 else 0
    
    # 超额收益
    excess_returns = returns - benchmark_returns
    excess_total = np.prod(1 + excess_returns) - 1
    
    # Alpha (年化)
    alpha = annual_return - 0.04  # 假设无风险利率4%
    
    # Beta
    if np.std(benchmark_returns) > 0:
        covariance = np.cov(returns, benchmark_returns)[0, 1]
        variance = np.var(benchmark_returns)
        beta = covariance / variance
    else:
        beta = 1.0
    
    # 夏普比率
    if np.std(returns) > 0:
        sharpe = (annual_return - 0.04) / (np.std(returns) * np.sqrt(12))
    else:
        sharpe = 0
    
    # 最大回撤
    cumulative = np.cumprod(1 + returns)
    peak = np.maximum.accumulate(cumulative)
    drawdown = (cumulative - peak) / peak
    max_dd = drawdown.min()
    
    # 胜率
    win_rate = len(returns[returns > 0]) / len(returns)
    
    # 盈亏比
    avg_win = np.mean(returns[returns > 0]) if len(returns[returns > 0]) > 0 else 0
    avg_loss = np.mean(returns[returns < 0]) if len(returns[returns < 0]) > 0 else 0
    profit_loss_ratio = abs(avg_win / avg_loss) if avg_loss != 0 else 0
    
    # 信息比率 (超额收益/跟踪误差)
    if np.std(excess_returns) > 0:
        info_ratio = np.mean(excess_returns) / (np.std(excess_returns) * np.sqrt(12))
    else:
        info_ratio = 0
    
    return {
        'total_return': total_return,
        'annual_return': annual_return,
        'excess_return': excess_total,
        'alpha': alpha,
        'beta': beta,
        'sharpe': sharpe,
        'max_drawdown': max_dd,
        'win_rate': win_rate,
        'profit_loss_ratio': profit_loss_ratio,
        'info_ratio': info_ratio,
        'n_trades': len(returns)
    }

def main():
    print("="*60)
    print("5年完整回测 (2019-2026)")
    print("="*60)
    
    # 加载数据
    data = load_index_data()
    
    for name, df in data.items():
        print(f"{name}: {df['date'].min()} ~ {df['date'].max()}, {len(df)}条")
    
    # 运行策略
    returns, benchmark_returns = strategy_etf_rotation(data)
    
    if not returns:
        print("无回测数据")
        return
    
    # 计算指标
    metrics = calculate_metrics(returns, benchmark_returns)
    
    # 基准收益
    benchmark_total = np.prod(1 + np.array(benchmark_returns)) - 1
    benchmark_annual = (1 + benchmark_total) ** (12/len(benchmark_returns)) - 1
    
    # 打印结果
    print("\n" + "="*60)
    print("回测结果")
    print("="*60)
    print(f"回测区间: 2019-01 ~ 2026-03")
    print(f"调仓次数: {metrics['n_trades']}")
    print()
    print("【收益】")
    print(f"  总收益率: {metrics['total_return']*100:+.2f}%")
    print(f"  年化收益率: {metrics['annual_return']*100:+.2f}%")
    print(f"  基准(中证1000)年化: {benchmark_annual*100:+.2f}%")
    print(f"  超额收益: {(metrics['annual_return'] - benchmark_annual)*100:+.2f}%")
    print()
    print("【风险指标】")
    print(f"  Alpha: {metrics['alpha']*100:+.2f}%")
    print(f"  Beta: {metrics['beta']:.3f}")
    print(f"  夏普比率: {metrics['sharpe']:.3f}")
    print(f"  最大回撤: {metrics['max_drawdown']*100:.2f}%")
    print(f"  信息比率: {metrics['info_ratio']:.3f}")
    print()
    print("【交易指标】")
    print(f"  胜率: {metrics['win_rate']*100:.1f}%")
    print(f"  盈亏比: {metrics['profit_loss_ratio']:.2f}")
    
    print("\n" + "="*60)
    print("指标解释")
    print("="*60)
    print("""
Alpha: 策略超越基准的超额收益 (年化)
Beta:  策略相对于市场的波动程度
       >1 波动更大, <1 波动更小
夏普:  风险调整后收益, >1 较好
信息比率: 超额收益稳定性, >0.5 较好
    """)

if __name__ == '__main__':
    main()
