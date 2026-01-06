import pandas as pd
import numpy as np
import akshare as ak
from clickhouse_driver import Client
from datetime import datetime, timedelta
import time
import random
from tqdm import tqdm

# Config
CH_HOST = '...'
CH_DB = 'stock_data'

START_DATE = '2020-01-01'
MOMENTUM_WINDOW = 5
VOL_WINDOW = 20
TOP_K_CONCEPTS = 5
ALPHA_SCALE = 1.0

# 全局缓存: 避免重复请求同一个板块的成分股
# format: {'板块名称': ['000001', '600519', ...]}
CONCEPT_STOCKS_CACHE = {}

def get_client():
    return Client(host=CH_HOST, database=CH_DB, settings={'use_numpy': True})

def fetch_all_concept_history():
    print(f"[1/4] Fetching concept history since {START_DATE}...")
    client = get_client()
    
    # 多取一点数据用于计算初始的 MA
    query_start = (datetime.strptime(START_DATE, "%Y-%m-%d") - timedelta(days=60)).strftime("%Y-%m-%d")
    
    sql = f"""
    SELECT trade_date, concept_code, concept_name, close, vol 
    FROM stock_concept_daily 
    WHERE trade_date >= '{query_start}'
    ORDER BY trade_date ASC
    """
    
    data = client.execute(sql)
    df = pd.DataFrame(data, columns=['trade_date', 'concept_code', 'concept_name', 'close', 'vol'])
    df['trade_date'] = pd.to_datetime(df['trade_date'])
    df = df.drop_duplicates(subset=['trade_date', 'concept_code'])
    
    print(f"Loaded {len(df)} rows of concept data.")

    return df

def get_stocks_in_concept_cached(concept_name):
    """带内存缓存的成分股获取函数"""
    if concept_name in CONCEPT_STOCKS_CACHE:
        return CONCEPT_STOCKS_CACHE[concept_name]
    
    try:
        df = ak.stock_board_concept_cons_em(symbol=concept_name)
        if df is not None and not df.empty:
            stocks = df['代码'].astype(str).tolist()
            CONCEPT_STOCKS_CACHE[concept_name] = stocks
            time.sleep(0.2) # 防封
            return stocks
    except Exception as e:
        print(f"Error fetching {concept_name}: {e}")
    
    CONCEPT_STOCKS_CACHE[concept_name] = [] # 即使失败也缓存空列表, 防止死循环重试
    return []

def main():
    # 1. 获取所有历史数据
    df = fetch_all_concept_history()
    if df.empty: return

    print("[2/4] Calculating historical scores...")
    # Pivot
    df_close = df.pivot(index='trade_date', columns='concept_code', values='close')
    df_vol = df.pivot(index='trade_date', columns='concept_code', values='vol')
    
    # Vectorized Calculation (一次性算出所有日期的因子)
    momentum = df_close.pct_change(MOMENTUM_WINDOW)
    vol_ma = df_vol.rolling(window=VOL_WINDOW).mean()
    vol_ratio = df_vol / (vol_ma + 1e-9)
    
    # 还原名称映射
    code_name_map = df[['concept_code', 'concept_name']].drop_duplicates().set_index('concept_code')['concept_name'].to_dict()

    # 准备写入的数据列表
    batch_data = []
    
    # 获取有效日期列表 (从 START_DATE 开始)
    valid_dates = momentum.index[momentum.index >= pd.Timestamp(START_DATE)]
    
    print(f"[3/4] iterating through {len(valid_dates)} days to map stocks...")
    
    for current_date in tqdm(valid_dates):
        try:
            # 提取当天的切片
            day_mom = momentum.loc[current_date].dropna()
            day_vol = vol_ratio.loc[current_date].dropna()
            
            common_idx = day_mom.index.intersection(day_vol.index)
            if len(common_idx) == 0: continue
            
            # 截面排名
            rank_mom = day_mom[common_idx].rank(pct=True)
            rank_vol = day_vol[common_idx].rank(pct=True)
            final_score = 0.7 * rank_mom + 0.3 * rank_vol
            
            # 取当天 Top K 板块
            top_concepts = final_score.nlargest(TOP_K_CONCEPTS)
            
            # 将板块分映射给成分股
            day_signals = []
            for code, score in top_concepts.items():
                c_name = code_name_map.get(code)
                if not c_name: continue
                
                # 获取成分股 (优先查缓存)
                stocks = get_stocks_in_concept_cached(c_name)
                
                for stock_code in stocks:
                    day_signals.append({
                        'ts_code': stock_code,
                        'trade_date': current_date.date(),
                        'alpha_score': score * ALPHA_SCALE,
                        'strategy_name': 'sector_rotation_v1'
                    })
            
            # 聚合去重 (如果一只股票同时属于两个Top板块，取最大分)
            if day_signals:
                df_day = pd.DataFrame(day_signals)
                # 按股票去重取最大值
                df_day_final = df_day.groupby(['ts_code', 'trade_date', 'strategy_name'])['alpha_score'].max().reset_index()
                batch_data.append(df_day_final)
                
        except KeyError:
            continue

    if not batch_data:
        print("No signals generated.")
        return

    print("[4/4] Merging and inserting into ClickHouse...")
    final_df = pd.concat(batch_data, ignore_index=True)
    
    # 写入数据库
    client = get_client()
    
    # 为了保证数据纯净, 先删除旧的历史数据 (保留表结构)
    # 如果想保留之前的实盘记录, 可以只删除 START_DATE 之后的数据

    ALLOW_DELETE = os.getenv("ALLOW_DELETE", "0") == "1"

    if ALLOW_DELETE:
        print(f"Deleting old sector_rotation_v1 data since {START_DATE}...")
        client.execute(
            f"ALTER TABLE stock_daily_alpha DELETE "
            f"WHERE strategy_name = 'sector_rotation_v1' AND trade_date >= '{START_DATE}'"
        )
    else:
        print("Skip DELETE. Set ALLOW_DELETE=1 to enable deletion.")
    
    #print(f"Deleting old sector_rotation_v1 data since {START_DATE}...")
    #client.execute(f"ALTER TABLE stock_daily_alpha DELETE WHERE strategy_name = 'sector_rotation_v1' AND trade_date >= '{START_DATE}'")
    
    # 分批写入防止超时
    chunk_size = 50000
    total_rows = len(final_df)
    print(f"Inserting {total_rows} rows...")
    
    for i in range(0, total_rows, chunk_size):
        chunk = final_df.iloc[i:i+chunk_size]
        client.insert_dataframe(
            'INSERT INTO stock_daily_alpha (ts_code, trade_date, strategy_name, alpha_score) VALUES',
            chunk[['ts_code', 'trade_date', 'strategy_name', 'alpha_score']]
        )
        print(f" Written {i + len(chunk)} / {total_rows}...")

    print("Historical Backfill Complete!")

if __name__ == "__main__":
    main()
