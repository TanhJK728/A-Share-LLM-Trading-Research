import pandas as pd
import pymongo
import akshare as ak
import jieba
from datetime import datetime
from clickhouse_driver import Client
from nlp_stocks import load_resources, BLACKLIST
from llm_judge import analyze_news_impact

# Connect to Database
mongo_client = pymongo.MongoClient("...")
news_collection = mongo_client["stock_data"]["news_cailianshe"]
# ClickHouse Connection
ch_client = Client(host='...', user='...', password='...', database='stock_data', settings={'use_numpy': True})

def get_market_caps(stock_codes):
    """
    批量获取股票的最新市值 (RAG 的核心数据源)
    """
    print("正在查询最新市值数据...")
    try:
        # 获取全市场实时行情
        df = ak.stock_zh_a_spot_em()
        # 筛选出需要的股票
        df = df[df['代码'].isin(stock_codes)]
        
        # map: code -> market_cap (亿元)
        # AKShare 返回的 '总市值' 单位是元，我们需要转成亿元方便 AI 理解
        market_cap_map = {}
        for _, row in df.iterrows():
            code = row['代码']
            # 总市值可能很大，转为“亿”为单位，保留2位小数
            mkt_cap_yi = round(row['总市值'] / 100000000, 2)
            market_cap_map[code] = mkt_cap_yi
            
        return market_cap_map
    except Exception as e:
        print(f"获取市值失败: {e}")
        return {}
    
def save_results(results):
    """
    将分析结果批量写入 ClickHouse
    """
    if not results: return

    print(f"正在将 {len(results)} 条因子数据存入 ClickHouse...")
    
    data_to_insert = []
    today = datetime.now().date()
    
    for res in results:
        # 构造一行数据
        row = {
            'ts_code': res['code'],
            'trade_date': today, # 实际应取新闻发布时间对应的交易日，这里先简化为今天
            'publish_time': res['publish_time'],
            'news_title': res['title'],
            'score': res['score'],
            'magnitude': res.get('magnitude', 0.0),
            'certainty': res.get('certainty', 0.0),
            'reason': res['reason']
        }
        data_to_insert.append(row)
    
    # 转 DataFrame
    df = pd.DataFrame(data_to_insert)
    
    try:
        ch_client.insert_dataframe(
            'INSERT INTO stock_news_sentiment (ts_code, trade_date, publish_time, news_title, score, magnitude, certainty, reason) VALUES',
            df
        )
        print("因子入库成功！")
    except Exception as e:
        print(f"入库失败: {e}")


def run_ai_strategy():
    maps = load_resources()
    if not maps: return
    alias_map, name_map = maps

    print("\n📰 1. 扫描最近新闻...")
    # 扫描最近 20 条用于测试
    recent_news = list(news_collection.find().sort("crawled_at", -1).limit(20))
    stock_news_map = {} 

    # News time
    for news in recent_news:
        content = news.get('content') or news.get('内容') or ''
        title = news.get('title') or news.get('标题') or '快讯'
    
        pub_time_str = news.get('publish_time') or news.get('time')
    
        try:
            if pub_time_str:
                if len(pub_time_str) > 10:
                    pub_time = datetime.strptime(str(pub_time_str), "%Y-%m-%d %H:%M:%S")
                else:
                    pub_time = datetime.now() # 只有时分秒的情况暂略
            else:
                pub_time = datetime.now()
        except:
            pub_time = datetime.now()

        full_text = f"{title} {content}"
        words = jieba.lcut(full_text)
        seen_in_this_news = set()
        
        for w in words:
            if w in BLACKLIST or len(w) < 2: continue
            if w in alias_map:
                code = alias_map[w]
                if code not in seen_in_this_news:
                    if code not in stock_news_map:
                        stock_news_map[code] = []
                    # tuple (内容, 标题, 时间)
                    stock_news_map[code].append((full_text[:500], title, pub_time))
                    seen_in_this_news.add(code)

    if not stock_news_map:
        print("没有检测到相关股票。")
        return

    # RAG
    target_codes = list(stock_news_map.keys())
    market_cap_map = get_market_caps(target_codes)
    print(f"找到 {len(stock_news_map)} 只股票, 开始 AI 评分...")
    
    results = []

    # LLM
    for code, items in stock_news_map.items():
        name = name_map.get(code, "未知")
        market_cap = market_cap_map.get(code, "未知")
        
        # 只分析最新的一条
        latest_item = items[0] 
        news_content = latest_item[0]
        news_title = latest_item[1]
        pub_time = latest_item[2]
        
        # 调用 AI
        ai_result = analyze_news_impact(name, code, market_cap, news_content)
        
        results.append({
            'code': code,
            'name': name,
            'publish_time': pub_time,
            'title': news_title,
            'score': ai_result['final_score'],
            'magnitude': ai_result.get('magnitude', 0),
            'certainty': ai_result.get('certainty', 0),
            'reason': ai_result['reason']
        })

    save_results(results)

if __name__ == "__main__":
    run_ai_strategy()
