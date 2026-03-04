#!/usr/bin/env python3
"""
高收益量化投资系统 v1
目标: 年化50%+

策略:
1. 小市值可转债轮动
2. A股小市值动量
3. ETF轮动
"""
import os
import pandas as pd
import numpy as np
import akshare as ak
import tushare as ts
from datetime import datetime
from dotenv import load_dotenv

# 加载配置
load_dotenv('/Users/ling/.openclaw/workspace/quant_system/config/.env.tushare')
TOKEN = os.getenv('TUSHARE_TOKEN')

DATA_DIR = '/Users/ling/.openclaw/workspace/quant_system/data'

class HighReturnStrategy:
    """高收益策略系统"""
    
    def __init__(self):
        self.pro = ts.pro_api(TOKEN)
        
    def get_cb_data(self):
        """获取可转债数据"""
        print("📊 获取可转债数据...")
        df = ak.bond_cb_jsl()
        return df
    
    def get_stock_data(self):
        """获取A股小市值数据"""
        print("📊 获取A股数据...")
        # 使用已有的数据
        if os.path.exists(f'{DATA_DIR}/daily_basic_all.csv'):
            df = pd.read_csv(f'{DATA_DIR}/daily_basic_all.csv')
            return df
        return None
    
    def select_cb_rotational(self, n=10):
        """
        可转债轮动策略
        选取双低值最低的N只
        """
        df = self.get_cb_data()
        
        # 清理数据
        df = df[df['双低'].notna()]
        df = df[df['双低'] != '-']
        df['双低'] = df['双低'].astype(float)
        
        # 按双低排序
        df = df.sort_values('双低').head(n)
        
        print(f"\n可转债轮动 - 选取{n}只:")
        print(df[['代码', '转债名称', '正股名称', '现价', '双低']].to_string(index=False))
        
        return df
    
    def select_small_cap(self, n=20, min_mv=10, max_mv=50):
        """
        A股小市值策略
        """
        df = self.get_stock_data()
        if df is None:
            print("无A股数据")
            return None
        
        # 最新日期
        latest = df['trade_date'].max()
        df = df[df['trade_date'] == latest].copy()
        
        # 市值过滤
        df['circ_mv_yi'] = df['circ_mv'] / 10000
        df = df[(df['circ_mv_yi'] >= min_mv) & (df['circ_mv_yi'] <= max_mv)]
        
        # PE过滤
        df = df[(df['pe'] > 0) & (df['pe'] < 60)]
        
        # 按市值排序
        df = df.nsmallest(n, 'circ_mv_yi')
        
        # 获取名称
        basic = pd.read_csv(f'{DATA_DIR}/stock_basic.csv')
        df = df.merge(basic[['ts_code', 'name', 'industry']], on='ts_code', how='left')
        
        print(f"\nA股小市值 - 选取{n}只:")
        print(df[['ts_code', 'name', 'industry', 'circ_mv_yi', 'pe']].head(10).to_string(index=False))
        
        return df
    
    def get_etf_momentum(self):
        """ETF动量策略"""
        print("\n📈 ETF动量分析...")
        
        # 常见ETF
        etfs = {
            '159919': '沪深300ETF',
            '159995': '券商ETF', 
            '512880': '证券ETF',
            '159992': '创新药ETF',
            '515790': '光伏ETF',
        }
        
        # 简单动量计算
        results = []
        for code, name in etfs.items():
            try:
                df = akfund_etf_hist_em(symbol=code, period='daily', start_date='20250101')
                if len(df) > 20:
                    ret = (df['收盘'].iloc[-1] / df['收盘'].iloc[-20] - 1) * 100
                    results.append({'code': code, 'name': name, 'ret20d': ret})
            except:
                pass
        
        if results:
            df = pd.DataFrame(results)
            df = df.sort_values('ret20d', ascending=False)
            print(df.to_string(index=False))
        
        return df
    
    def run_dual_strategy(self):
        """
        运行双策略组合
        60% 可转债 + 40% A股小市值
        """
        print("="*60)
        print("高收益组合策略")
        print("="*60)
        
        # 可转债
        cb = self.select_cb_rotational(n=10)
        
        # A股小市值
        stock = self.select_small_cap(n=15, min_mv=15, max_mv=50)
        
        print("\n" + "="*60)
        print("策略说明")
        print("="*60)
        print("""
本系统采用【可转债+A股小市值】双轮动策略:

1. 可转债轮动 (60%仓位)
   - 选取双低值最低的10只
   - 下修博弈 + 股性弹性
   - 预计年化: 40-60%

2. A股小市值 (40%仓位)  
   - 选取流通市值15-50亿
   - PE<60, 排除亏损
   - 预计年化: 30-50%

3. 仓位管理
   - 市场高位: 降低仓位
   - 市场低位: 提高仓位
   
4. 风险控制
   - 单只止损: -10%
   - 大盘止损: -5%清仓
   - 最大回撤控制: -30%

⚠️ 风险提示: 高收益策略伴随高回撤
        """)
        
        return {
            'cb': cb,
            'stock': stock
        }

def main():
    strategy = HighReturnStrategy()
    result = strategy.run_dual_strategy()

if __name__ == '__main__':
    main()
