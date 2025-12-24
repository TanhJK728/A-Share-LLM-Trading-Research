# A-Share-LLM-Trading-Research
Based on Emotion


graph TD
    User -->|查看| Tabix(Tabix 网页看板)
    User -->|编写| Research(策略脚本 / research)
    
subgraph "自动数据工厂 (Mac后台)"
        Cron(Crontab 定时任务) -->|16:00 触发| Script1(fetch_akshare.py)
        Cron -->|22:00 触发| Script2(fetch_news.py)
    end
    
subgraph "数据管道"
        Script1 -->|抓取行情| Internet1(东方财富 AKShare)
        Script2 -->|抓取新闻| Internet2(财联社 AKShare)
    end
    
subgraph "核心数据仓库 (Docker)"
        Script1 -->|存入| ClickHouse[(ClickHouse 数据库)]
        Script2 -->|存入| MongoDB[(MongoDB 数据库)]
    end
    
subgraph "大脑 (策略层)"
        ClickHouse -->|提供价格| Strategy(均线回测 / NLP热点)
        MongoDB -->|提供舆情| Strategy
    end


每天在终端输入: sh daily_work.sh


全自动的系统：

收盘后：跑 strategy_technical.py 算分。

交易时：跑 mock_trader_full.py。

不断的优胜劣汰，让资金永远流向概率最高的地方。
