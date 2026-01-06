import akshare as ak
import pandas as pd
from datetime import datetime, timedelta
from clickhouse_driver import Client
from tqdm import tqdm
import time
import random
import requests

# 连接 ClickHouse
client = Client(host='..., user='...', password='', database='stock_data', settings={'use_numpy': True})

# 建表
client.execute("""
CREATE TABLE IF NOT EXISTS stock_data.stock_concept_daily
(
    `concept_name` LowCardinality(String),
    `concept_code` String,
    `trade_date` Date CODEC(DoubleDelta, ZSTD(1)),
    `open` Float64,
    `high` Float64,
    `low` Float64,
    `close` Float64,
    `vol` Float64,
    `amount` Float64,
    `pct_chg` Float64
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(trade_date)
ORDER BY (concept_name, trade_date)
SETTINGS index_granularity = 8192;
""")

def get_all_concepts():
    print("正在获取概念板块列表...")
    for i in range(5):
        try:
            df = ak.stock_board_concept_name_em()
            return df
        except Exception:
            time.sleep(2)
    return pd.DataFrame()

def get_concept_status():
    """
    检查每个板块最新下载到了哪一天, return dictionary: {'板块名': '2025-01-01'}
    """
    try:
        sql = "SELECT concept_name, max(trade_date) FROM stock_concept_daily GROUP BY concept_name"
        result = client.execute(sql)
        return {row[0]: row[1] for row in result}
    except Exception:
        return {}

def download_concept_history(concept_name, concept_code, start_date):
    end_date = datetime.now().strftime("%Y%m%d")
    s_date_str = start_date.strftime("%Y%m%d")
    
    if s_date_str > end_date:
        return False

    for attempt in range(3):
        try:
            # 获取数据
            df = ak.stock_board_concept_hist_em(
                symbol=concept_name, 
                period="daily", 
                start_date=s_date_str, 
                end_date=end_date, 
                adjust=""
            )
            
            if df is None or df.empty: return False

            # 清洗
            rename_dict = {
                '日期': 'trade_date', '开盘': 'open', '最高': 'high', '最低': 'low',
                '收盘': 'close', '成交量': 'vol', '成交额': 'amount', '涨跌幅': 'pct_chg'
            }
            df = df.rename(columns=rename_dict)
            df['concept_name'] = concept_name
            df['concept_code'] = str(concept_code)
            df['trade_date'] = pd.to_datetime(df['trade_date']).dt.date
            
            df = df[df['trade_date'] >= start_date]
            if df.empty: return False

            # all to numeric
            cols = ['open', 'high', 'low', 'close', 'vol', 'amount', 'pct_chg']
            for c in cols:
                df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0)

            final_cols = ['concept_name', 'concept_code', 'trade_date', 'open', 'high', 'low', 'close', 'vol', 'amount', 'pct_chg']
            
            client.insert_dataframe(
                'INSERT INTO stock_concept_daily (concept_name, concept_code, trade_date, open, high, low, close, vol, amount, pct_chg) VALUES',
                df[final_cols]
            )
            return True
        except Exception:
            time.sleep(1)
            
    return False

if __name__ == "__main__":
    concepts_df = get_all_concepts()
    if concepts_df.empty:
        print("无法获取板块列表")
        exit()

    # 获取当前数据库里的状态
    status_map = get_concept_status()
    today = datetime.now().date()
    default_start = datetime(2020, 1, 1).date()

    tasks = []
    
    # 生成任务列表
    for index, row in concepts_df.iterrows():
        c_name = row['板块名称']
        c_code = row['板块代码']
        
        last_date = status_map.get(c_name)
        
        if last_date is None:
            # 1. 全新板块: 从 2020 开始下载
            tasks.append((c_name, c_code, default_start))
        elif last_date < today:
            # 2. 旧板块: 从上次结束的第二天开始下载
            next_day = last_date + timedelta(days=1)
            tasks.append((c_name, c_code, next_day))
        # 3. 如果 last_date == today, 说明已经更新过了，跳过

    print(f"任务统计：总板块 {len(concepts_df)}, 需更新 {len(tasks)}")
    
    if not tasks:
        print("所有板块数据已是最新, 无需更新。")
        exit()

    # 进度条
    pbar = tqdm(tasks)
    for name, code, start_date in pbar:
        pbar.set_description(f"更新 {name}")
        download_concept_history(name, code, start_date)
        time.sleep(random.uniform(0.2, 0.5)) # 稍微休眠，防封
            
    print("板块数据更新完成!")
