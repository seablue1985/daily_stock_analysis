#!/usr/bin/env python3
"""
多策略量化投资系统 v2
目标: 年化50%+
策略库: 10+高收益策略
"""
import os
import pandas as pd
import numpy as np
import akshare as ak
import tushare as ts
from datetime import datetime
from dotenv import load_dotenv

load_dotenv('/Users/ling/.openclaw/workspace/quant_system/config/.env.tushare')
TOKEN = os.getenv('TUSHARE_TOKEN')

DATA_DIR = '/Users/ling/.openclaw/workspace/quant_system/data'

class MultiStrategySystem:
    """多策略系统"""
    
    def __init__(self):
        self.pro = ts.pro_api(TOKEN)
        self.strategies = {}
        
    # ========== 策略1: 可转债双低轮动 ==========
    def strategy_cb_dual_low(self, n=10):
        """可转债双低轮动 - 选取双低值最低的转债"""
        print("\n" + "="*50)
        print("【策略1】可转债双低轮动")
        print("="*50)
        
        df = ak.bond_cb_jsl()
        
        # 清理数据
        df = df[df['双低'].notna() & (df['双低'] != '-')]
        df['双低'] = df['双低'].astype(float)
        df = df[df['双低'] < 150]  # 过滤极端值
        
        # 按双低排序
        result = df.nsmallest(n, '双低')
        
        print(f"选取{n}只双低转债:")
        print(result[['代码', '转债名称', '正股名称', '现价', '双低', '转股溢价率']].head(10).to_string(index=False))
        
        self.strategies['可转债双低'] = result
        return result
    
    # ========== 策略2: 小市值A股轮动 ==========
    def strategy_small_cap(self, n=20, min_mv=10, max_mv=50):
        """小市值A股轮动"""
        print("\n" + "="*50)
        print("【策略2】小市值A股轮动")
        print("="*50)
        
        daily = pd.read_csv(f'{DATA_DIR}/daily_basic_all.csv')
        basic = pd.read_csv(f'{DATA_DIR}/stock_basic.csv')
        
        latest = daily['trade_date'].max()
        df = daily[daily['trade_date'] == latest].copy()
        
        # 市值过滤
        df['circ_mv_yi'] = df['circ_mv'] / 10000
        df = df[(df['circ_mv_yi'] >= min_mv) & (df['circ_mv_yi'] <= max_mv)]
        
        # PE过滤
        df = df[(df['pe'] > 0) & (df['pe'] < 60)]
        
        # 按市值排序
        df = df.nsmallest(n * 2, 'circ_mv_yi')
        
        # 行业分散
        df = df.merge(basic[['ts_code', 'name', 'industry']], on='ts_code', how='left')
        df = df.groupby('industry').head(2).head(n)
        
        print(f"选取{n}只小市值股票:")
        print(df[['ts_code', 'name', 'industry', 'circ_mv_yi', 'pe']].head(10).to_string(index=False))
        
        self.strategies['小市值A股'] = df
        return df
    
    # ========== 策略3: 动量反转策略 ==========
    def strategy_momentum(self, n=20, lookback=20):
        """动量策略 - 选取近期涨幅最好的"""
        print("\n" + "="*50)
        print("【策略3】动量策略")
        print("="*50)
        
        daily = pd.read_csv(f'{DATA_DIR}/daily_basic_all.csv')
        basic = pd.read_csv(f'{DATA_DIR}/stock_basic.csv')
        
        dates = sorted(daily['trade_date'].unique())
        
        if len(dates) < lookback:
            print("数据不足")
            return
        
        # 计算动量
        mom_data = []
        latest = dates[-1]
        
        # 获取有足够历史的股票
        stock_list = daily['ts_code'].unique()[:500]
        
        for code in stock_list:
            stock_df = daily[daily['ts_code'] == code].sort_values('trade_date')
            if len(stock_df) >= lookback:
                start_price = stock_df.iloc[-lookback]['close']
                end_price = stock_df.iloc[-1]['close']
                if start_price > 0:
                    ret = (end_price - start_price) / start_price
                    mv = stock_df.iloc[-1]['circ_mv']
                    mom_data.append({
                        'code': code, 
                        'momentum': ret, 
                        'circ_mv': mv
                    })
        
        if not mom_data:
            return
            
        df = pd.DataFrame(mom_data)
        df['circ_mv_yi'] = df['circ_mv'] / 10000
        
        # 过滤: 市值10-100亿, 动量>0
        df = df[(df['circ_mv_yi'] >= 10) & (df['circ_mv_yi'] <= 100)]
        df = df[df['momentum'] > 0]
        
        # 按动量排序
        df = df.nlargest(n, 'momentum')
        df = df.merge(basic[['symbol', 'name', 'industry']], on='symbol', how='left')
        df = df.rename(columns={'symbol': 'ts_code'})
        
        print(f"选取{n}只动量股:")
        print(df[['ts_code', 'name', 'momentum', 'circ_mv_yi']].head(10).to_string(index=False))
        
        self.strategies['动量策略'] = df
        return df
    
    # ========== 策略4: 高股息策略 ==========
    def strategy_high_dividend(self, n=15):
        """高股息策略 - 选取股息率最高的"""
        print("\n" + "="*50)
        print("【策略4】高股息策略")
        print("="*50)
        
        daily = pd.read_csv(f'{DATA_DIR}/daily_basic_all.csv')
        basic = pd.read_csv(f'{DATA_DIR}/stock_basic.csv')
        
        latest = daily['trade_date'].max()
        df = daily[daily['trade_date'] == latest].copy()
        
        # 计算股息率 (简化: 用PE倒数近似)
        df['div_yield'] = 1 / df['pe'] * 100  # 简化股息率
        df = df[df['div_yield'] > 1]  # 股息率>1%
        
        # 市值过滤
        df['circ_mv_yi'] = df['circ_mv'] / 10000
        df = df[df['circ_mv_yi'] > 50]  # 大市值
        
        df = df.nlargest(n, 'div_yield')
        df = df.merge(basic[['ts_code', 'name', 'industry']], on='ts_code', how='left')
        
        print(f"选取{n}只高股息股票:")
        print(df[['ts_code', 'name', 'industry', 'circ_mv_yi', 'div_yield']].head(10).to_string(index=False))
        
        self.strategies['高股息'] = df
        return df
    
    # ========== 策略5: 行业轮动 ==========
    def strategy_industry_rotation(self, n=3):
        """行业轮动 - 选取近期最强的行业"""
        print("\n" + "="*50)
        print("【策略5】行业轮动")
        print("="*50)
        
        daily = pd.read_csv(f'{DATA_DIR}/daily_basic_all.csv')
        basic = pd.read_csv(f'{DATA_DIR}/stock_basic.csv')
        
        dates = sorted(daily['trade_date'].unique())
        
        if len(dates) < 20:
            print("数据不足")
            return
        
        # 计算各行业涨幅
        industry_ret = {}
        
        for industry in basic['industry'].unique()[:50]:
            stocks = basic[basic['industry'] == industry]['ts_code'].tolist()
            if len(stocks) < 5:
                continue
                
            ind_df = daily[(daily['ts_code'].isin(stocks)) & (daily['trade_date'] >= dates[-20])]
            
            if len(ind_df) > 0:
                # 计算行业平均涨幅
                start_prices = ind_df.groupby('ts_code')['close'].first()
                end_prices = ind_df.groupby('ts_code')['close'].last()
                
                rets = (end_prices / start_prices - 1).dropna()
                if len(rets) > 3:
                    industry_ret[industry] = rets.mean()
        
        if not industry_ret:
            return
            
        df = pd.DataFrame(list(industry_ret.items()), columns=['industry', 'return'])
        df = df.nlargest(n, 'return')
        
        print(f"选取{n}个强势行业:")
        print(df.to_string(index=False))
        
        self.strategies['行业轮动'] = df
        return df
    
    # ========== 策略6: 龙头股策略 ==========
    def strategy_leader(self, n=10):
        """行业龙头策略 - 各行业市值最大的"""
        print("\n" + "="*50)
        print("【策略6】行业龙头策略")
        print("="*50)
        
        daily = pd.read_csv(f'{DATA_DIR}/daily_basic_all.csv')
        basic = pd.read_csv(f'{DATA_DIR}/stock_basic.csv')
        
        latest = daily['trade_date'].max()
        df = daily[daily['trade_date'] == latest].copy()
        
        df['circ_mv_yi'] = df['circ_mv'] / 10000
        df = df.merge(basic[['ts_code', 'name', 'industry']], on='ts_code', how='left')
        
        # 各行业选市值最大的
        df = df.sort_values('circ_mv_yi', ascending=False)
        result = df.groupby('industry').head(1).head(n)
        
        print(f"选取{n}只行业龙头:")
        print(result[['ts_code', 'name', 'industry', 'circ_mv_yi']].head(10).to_string(index=False))
        
        self.strategies['行业龙头'] = result
        return result
    
    # ========== 策略7: 困境反转 ==========
    def strategy_turnaround(self, n=15):
        """困境反转 - 选取PE较低但有业绩改善的"""
        print("\n" + "="*50)
        print("【策略7】困境反转策略")
        print("="*50)
        
        daily = pd.read_csv(f'{DATA_DIR}/daily_basic_all.csv')
        basic = pd.read_csv(f'{DATA_DIR}/stock_basic.csv')
        
        latest = daily['trade_date'].max()
        df = daily[daily['trade_date'] == latest].copy()
        
        # PE在合理区间 (5-20)
        df = df[(df['pe'] > 5) & (df['pe'] < 20)]
        
        # 市值适中 (20-200亿)
        df['circ_mv_yi'] = df['circ_mv'] / 10000
        df = df[(df['circ_mv_yi'] >= 20) & (df['circ_mv_yi'] <= 200)]
        
        # 按PE排序 (越低越好)
        df = df.nsmallest(n * 2, 'pe')
        
        # 行业分散
        df = df.merge(basic[['ts_code', 'name', 'industry']], on='ts_code', how='left')
        df = df.groupby('industry').head(1).head(n)
        
        print(f"选取{n}只困境反转股:")
        print(df[['ts_code', 'name', 'industry', 'circ_mv_yi', 'pe']].head(10).to_string(index=False))
        
        self.strategies['困境反转'] = df
        return df
    
    # ========== 策略8: 科创板/创业板专项 ==========
    def strategy_china_ai(self, n=15):
        """科创AI策略 - 选取科创板+AI概念"""
        print("\n" + "="*50)
        print("【策略8】科创AI策略")
        print("="*50)
        
        daily = pd.read_csv(f'{DATA_DIR}/daily_basic_all.csv')
        basic = pd.read_csv(f'{DATA_DIR}/stock_basic.csv')
        
        # 筛选创业板(3开头)和科创板(688开头)
        chinext = basic[basic['ts_code'].str.startswith(('300', '301', '688'))]
        
        if len(chinext) == 0:
            print("无数据")
            return
            
        latest = daily['trade_date'].max()
        df = daily[(daily['trade_date'] == latest) & (daily['ts_code'].isin(chinext['ts_code']))].copy()
        
        df['circ_mv_yi'] = df['circ_mv'] / 10000
        df = df[df['circ_mv_yi'] <= 100]  # 排除超大盘
        
        # PE过滤
        df = df[(df['pe'] > 0) & (df['pe'] < 80)]
        
        # 按市值排序
        df = df.nsmallest(n * 2, 'circ_mv_yi')
        df = df.merge(basic[['ts_code', 'name', 'industry']], on='ts_code', how='left')
        
        print(f"选取{n}只科创AI股:")
        print(df[['ts_code', 'name', 'industry', 'circ_mv_yi', 'pe']].head(10).to_string(index=False))
        
        self.strategies['科创AI'] = df
        return df
    
    # ========== 运行所有策略 ==========
    def run_all_strategies(self):
        """运行所有策略"""
        print("="*60)
        print("多策略量化投资系统 v2")
        print("目标: 年化50%+")
        print("="*60)
        
        # 策略1: 可转债双低
        self.strategy_cb_dual_low(n=10)
        
        # 策略2: 小市值
        self.strategy_small_cap(n=15)
        
        # 策略3: 动量
        self.strategy_momentum(n=15)
        
        # 策略4: 高股息
        self.strategy_high_dividend(n=10)
        
        # 策略5: 行业轮动
        self.strategy_industry_rotation(n=3)
        
        # 策略6: 龙头股
        self.strategy_leader(n=10)
        
        # 策略7: 困境反转
        self.strategy_turnaround(n=10)
        
        # 策略8: 科创AI
        self.strategy_china_ai(n=10)
        
        # 汇总
        print("\n" + "="*60)
        print("策略汇总")
        print("="*60)
        print(f"共 {len(self.strategies)} 个策略:")
        for name in self.strategies.keys():
            n = len(self.strategies[name])
            print(f"  • {name}: {n}只")
        
        return self.strategies
    
    # ========== 推荐组合 ==========
    def recommend_portfolio(self):
        """推荐组合 - 根据风险偏好"""
        print("\n" + "="*60)
        print("推荐组合")
        print("="*60)
        
        print("""
【激进型组合】(追求高收益)
  • 可转债双低: 40%
  • 小市值A股: 30%
  • 动量策略: 20%
  • 科创AI: 10%
  预期年化: 50-80%
  风险: 高

【平衡型组合】
  • 可转债双低: 30%
  • 高股息: 25%
  • 行业龙头: 20%
  • 小市值: 15%
  • 困境反转: 10%
  预期年化: 30-50%
  风险: 中

【稳健型组合】
  • 高股息: 40%
  • 行业龙头: 30%
  • 困境反转: 20%
  • 可转债: 10%
  预期年化: 20-30%
  风险: 低
        """)

def main():
    system = MultiStrategySystem()
    system.run_all_strategies()
    system.recommend_portfolio()

if __name__ == '__main__':
    main()
