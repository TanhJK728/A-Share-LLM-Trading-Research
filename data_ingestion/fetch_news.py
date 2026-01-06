import akshare as ak
import pandas as pd
from datetime import datetime, date, time
import pymongo
from pymongo.errors import DuplicateKeyError

# Config
MONGO_URI = "..."
DB_NAME = "stock_data"
COLLECTION_NAME = "news_cailianshe"

print("正在连接 MongoDB...")
client = pymongo.MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

print("MongoDB 连接成功")

def fetch_and_save_news():
    print("正在抓取财联社 7x24 小时电报...")
    
    try:
        df = ak.stock_info_global_cls()
        
        if df is None or df.empty:
            print("未抓取到新闻")
            return

        print(f"抓取到 {len(df)} 条快讯. ")
        
        inserted_count = 0
        
        for _, row in df.iterrows():
            news_item = row.to_dict()
            
            # 遍历所有字段，把 MongoDB 不认识的 date/time 对象转成字符串
            for key, value in news_item.items():
                if isinstance(value, (date, time)):
                    news_item[key] = str(value)
            
            # 补充抓取时间
            news_item['crawled_at'] = datetime.now()
            
            # 去重检查：用 '内容' 字段判断是否已存在
            content_val = news_item.get('content') or news_item.get('内容')
            
            if content_val:
                # 检查库里是否已有该内容
                if collection.find_one({"内容": content_val}) or collection.find_one({"content": content_val}):
                    continue
            
            collection.insert_one(news_item)
            inserted_count += 1
                
        print(f"入库完成！新增: {inserted_count} 条")
        
    except Exception as e:
        print(f"抓取失败: {e}")

if __name__ == "__main__":
    fetch_and_save_news()
    
    # 验证
    count = collection.count_documents({})
    print(f"\nMongoDB '{COLLECTION_NAME}' 表当前总文档数: {count}")
    
    if count > 0:
        latest = collection.find_one(sort=[("crawled_at", -1)])
        print("\n最新一条新闻预览: ")
        print(f"时间: {latest.get('发布时间') or latest.get('time')}")
        print(f"标题: {latest.get('标题') or latest.get('title')}")
        
        # 内容的前 50 个字
        content = latest.get('内容') or latest.get('content') or ''
        print(f"内容: {content[:50]}...")
