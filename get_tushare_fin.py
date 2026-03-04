#!/usr/bin/env python3
"""
获取Tushare财务数据
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

def main():
    log("=" * 50)
    log("开始获取Tushare财务数据")
    log("=" * 50)
    
    pro = ts.pro_api(TOKEN)
    
    # 获取股票列表
    try:
        stocks = pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name')
        log(f"获取到 {len(stocks)} 只股票")
    except Exception as e:
        log(f"获取股票列表失败: {e}")
        return
    
    # 只取前200只小市值股票
    all_fin = []
    
    # 获取财务指标
    log("开始获取财务指标...")
    for i, row in stocks.head(200).iterrows():
        ts_code = row['ts_code']
        if (i+1) % 20 == 0:
            log(f"进度: {i+1}/200")
        
        try:
            fin = pro.fina_indicator(
                ts_code=ts_code,
                fields='ts_code,end_date,net_profits,roe,net_profit_margin,eps'
            )
            if len(fin) > 0:
                all_fin.append(fin)
            time.sleep(0.1)  # 避免请求过快
        except Exception as e:
            pass
    
    if all_fin:
        fin_df = pd.concat(all_fin, ignore_index=True)
        fin_df.to_csv(f'{DATA_DIR}/financial_tushare.csv', index=False)
        log(f"✅ 成功获取 {len(fin_df)} 条财务数据")
        log(f"涉及股票数: {fin_df['ts_code'].nunique()}")
    else:
        log("⚠️ 没有获取到财务数据")

if __name__ == '__main__':
    main()
