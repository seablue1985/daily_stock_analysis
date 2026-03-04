#!/usr/bin/env python3
"""
每日量化策略信号推送
每天9:00自动执行
"""
import os
import sys
import pandas as pd
import numpy as np
import akshare as ak
from datetime import datetime

# 添加项目路径
sys.path.insert(0, '/Users/ling/.openclaw/workspace/quant_system')

from dotenv import load_dotenv
load_dotenv('/Users/ling/.openclaw/workspace/quant_system/config/.env.tushare')
import tushare as ts
TOKEN = os.getenv('TUSHARE_TOKEN')

DATA_DIR = '/Users/ling/.openclaw/workspace/quant_system/data'

def get_market_summary():
    """获取市场概况"""
    result = "📊 市场概况\n"
    result += "="*40 + "\n"
    
    indices = {
        '000300.SH': '沪深300',
        '000905.SH': '中证500',
        '000852.SH': '中证1000',
    }
    
    for code, name in indices.items():
        try:
            fname = f'{DATA_DIR}/index_{code.replace(".", "_")}.csv'
            if os.path.exists(fname):
                df = pd.read_csv(fname)
                if len(df) > 0:
                    latest = df.iloc[0]
                    change = latest.get('pct_chg', 0)
                    result += f"{name}: {latest['close']:.2f} ({change:+.2f}%)\n"
        except:
            pass
    
    return result

def get_cb_strategy():
    """可转债双低策略"""
    try:
        df = ak.bond_cb_jsl()
        df = df[df['双低'].notna() & (df['双低'] != '-')]
        df['双低'] = df['双低'].astype(float)
        df = df[df['双低'] < 150].nsmallest(5, '双低')
        
        result = "\n🎯 可转债双低 TOP5:\n"
        for _, row in df.iterrows():
            result += f"  {row['代码']} {row['转债名称'][:6]} 双低:{row['双低']:.1f}\n"
        return result
    except:
        return "\n🎯 可转债双低: 获取失败\n"

def get_small_cap_strategy():
    """小市值策略"""
    try:
        daily = pd.read_csv(f'{DATA_DIR}/daily_basic_all.csv')
        basic = pd.read_csv(f'{DATA_DIR}/stock_basic.csv')
        
        latest = daily['trade_date'].max()
        df = daily[daily['trade_date'] == latest].copy()
        
        df['circ_mv_yi'] = df['circ_mv'] / 10000
        df = df[(df['circ_mv_yi'] >= 10) & (df['circ_mv_yi'] <= 50)]
        df = df[(df['pe'] > 0) & (df['pe'] < 60)]
        df = df.nsmallest(10, 'circ_mv_yi')
        df = df.merge(basic[['ts_code', 'name']], on='ts_code', how='left')
        
        result = "\n📈 小市值A股 TOP10:\n"
        for _, row in df.iterrows():
            name = row.get('name', row['ts_code'])[:6]
            result += f"  {row['ts_code']} {name} {row['circ_mv_yi']:.1f}亿\n"
        return result
    except Exception as e:
        return f"\n📈 小市值A股: 获取失败\n"

def get_momentum_strategy():
    """动量策略"""
    try:
        daily = pd.read_csv(f'{DATA_DIR}/daily_basic_all.csv')
        basic = pd.read_csv(f'{DATA_DIR}/stock_basic.csv')
        
        dates = sorted(daily['trade_date'].unique())
        
        if len(dates) < 20:
            return "\n📊 动量策略: 数据不足\n"
        
        mom_data = []
        latest = dates[-1]
        
        for code in daily['ts_code'].unique()[:300]:
            stock_df = daily[(daily['ts_code'] == code) & (daily['trade_date'] >= dates[-20])]
            if len(stock_df) >= 10:
                start = stock_df.iloc[0]['close']
                end = stock_df.iloc[-1]['close']
                if start > 0:
                    ret = (end - start) / start
                    mom_data.append({'code': code, 'ret': ret})
        
        if mom_data:
            df = pd.DataFrame(mom_data)
            df = df.nlargest(5, 'ret')
            df = df.merge(basic[['ts_code', 'name']], on='ts_code', how='left')
            
            result = "\n🔥 动量策略 TOP5:\n"
            for _, row in df.iterrows():
                name = row.get('name', row['ts_code'])[:6]
                result += f"  {row['ts_code']} {name} {row['ret']*100:+.1f}%\n"
            return result
        
        return "\n🔥 动量策略: 无数据\n"
    except:
        return "\n🔥 动量策略: 获取失败\n"

def generate_daily_signal():
    """生成每日信号"""
    today = datetime.now().strftime('%Y-%m-%d')
    
    message = f"🧠 量化投资日报 - {today}\n"
    message += "="*40 + "\n"
    
    # 市场概况
    message += get_market_summary()
    
    # 策略信号
    message += get_cb_strategy()
    message += get_small_cap_strategy()
    message += get_momentum_strategy()
    
    # 风险提示
    message += "\n⚠️ 风险提示: 所有策略仅供参考,据此操作风险自担\n"
    
    return message

def main():
    message = generate_daily_signal()
    print(message)
    return message

if __name__ == '__main__':
    main()
