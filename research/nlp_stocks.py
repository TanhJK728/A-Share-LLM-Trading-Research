import jieba
import pandas as pd
import pymongo
from clickhouse_driver import Client
import os

# Path
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
DICT_PATH = os.path.join(CURRENT_DIR, "stock_dict.txt")

# 2. DB connection
mongo_client = pymongo.MongoClient("...")
news_collection = mongo_client["stock_data"]["news_cailianshe"]

ch_client = Client(host='...', user='...', password='...', database='stock_data', settings={'use_numpy': True})


def load_resources():
    print("正在加载 A 股实体知识库...")
    
    # 1. 让 Jieba 加载"股票字典"
    if os.path.exists(DICT_PATH):
        jieba.load_userdict(DICT_PATH)
        print(f"已加载自定义词典: {DICT_PATH}")

    # 2. 从 ClickHouse 把“别名 -> 代码”的映射表取出来放在内存里
    # 格式: {'茅台': '600519', '贵州茅台': '600519', '宁王': '300750'...}
    print("正在读取代码映射表...")
    df = ch_client.query_dataframe("SELECT alias, ts_code, name FROM stock_alias")
    
    # 转换成字典方便查询, 如果有重名（比如'平安'），这里简单处理取第一个。
    # 进阶的话需要结合上下文消歧 (Disambiguation)
    alias_map = dict(zip(df['alias'], df['ts_code']))
    name_map = dict(zip(df['ts_code'], df['name']))
    
    print(f"内存映射构建完成，包含 {len(alias_map)} 个别名。")
    return alias_map, name_map

# 定义黑名单：这些词虽然是股票名，但太容易和通用词混淆
BLACKLIST = {
    '标准', '统一', '太平洋', '太阳能', '光明', '万能', '方正', '美好', '幸福', 
    '中意', '精工', '诚信', '友好', '百货', '建设', '能源', '中国'
}

def analyze_stock_mentions(alias_map, name_map):
    print("\n正在扫描新闻中的个股...")
    
    recent_news = list(news_collection.find().sort("crawled_at", -1).limit(50))
    stock_counter = {} 
    
    for news in recent_news:
        content = news.get('content') or news.get('内容') or ''
        title = news.get('title') or news.get('标题') or ''
        full_text = f"{title} {content}"
        
        words = jieba.lcut(full_text)
        seen_in_this_news = set()
        
        for w in words:
            # 过滤逻辑：如果在黑名单里，或者是单字，直接跳过
            if w in BLACKLIST or len(w) < 2:
                continue
                
            if w in alias_map:
                code = alias_map[w]
                if code not in seen_in_this_news:
                    stock_counter[code] = stock_counter.get(code, 0) + 1
                    seen_in_this_news.add(code)

    print("\n=== 24小时个股舆情榜 === ")
    sorted_stocks = sorted(stock_counter.items(), key=lambda x: x[1], reverse=True)
    
    if not sorted_stocks:
        print("暂无个股被提及.")
    else:
        print(f"{'排名':<5} {'代码':<10} {'名称':<10} {'热度':<10}")
        print("-" * 45)
        for rank, (code, count) in enumerate(sorted_stocks[:10], 1):
            name = name_map.get(code, "未知")
            print(f"#{rank:<4} {code:<10} {name:<10} {count:<10}")

if __name__ == "__main__":
    maps = load_resources()
    if maps:
        alias_map, name_map = maps
        analyze_stock_mentions(alias_map, name_map)
