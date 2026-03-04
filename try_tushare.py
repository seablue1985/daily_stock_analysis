#!/usr/bin/env python3
"""
定时尝试获取Tushare财务数据
"""
import tushare as ts
import pandas as pd
import os
import time

DATA_DIR = '/Users/ling/.openclaw/workspace/quant_system/data'
TOKEN = 'fa7fe5039b58853bd0df3fc31f1770fc4a8cfaa79c6bff0473829bbc'
LOG_FILE = '/Users/ling/.openclaw/workspace/quant_system/data/tushare_log.txt'

def log(msg):
    with open(LOG_FILE, 'a') as f:
        f.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - {msg}\n")
    print(msg)

def try_get_financial_data():
    """尝试获取财务数据"""
    pro = ts.pro_api(TOKEN)
    
    # 获取最新财务指标 - 不需要ts_code，获取全部
    try:
        # 使用fina_indicator获取所有股票的财务指标
        fin_df = pro.fina_indicator(
            fields='ts_code,end_date,net_profits,roe,net_profit_margin,eps'
        )
        
        if len(fin_df) > 0:
            fin_df.to_csv(f'{DATA_DIR}/financial_tushare.csv', index=False)
            log(f"✅ 成功获取 {len(fin_df)} 条财务数据!")
            return True
    except Exception as e:
        log(f"❌ 获取失败: {e}")
    
    return False

def main():
    log("=" * 50)
    log("开始尝试获取Tushare财务数据")
    log("=" * 50)
    
    success = False
    for attempt in range(1, 11):
        log(f"尝试 {attempt}/10...")
        
        if try_get_financial_data():
            success = True
            break
        
        # 等待后重试
        time.sleep(5)
    
    if success:
        log("🎉 成功获取数据!")
    else:
        log("⚠️ 10次尝试都失败了")

if __name__ == '__main__':
    main()
