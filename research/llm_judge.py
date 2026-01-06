import json
import math
from openai import OpenAI
import os

# Config
API_KEY = "..." 
BASE_URL = "https://api...com"

client = OpenAI(api_key=API_KEY, base_url=BASE_URL)

def calculate_score(direction, magnitude, certainty):
    """ CS = D * tanh(M * C) """
    raw_impact = magnitude * certainty
    score = direction * math.tanh(raw_impact)
    return score

def analyze_news_impact(stock_name, stock_code, market_cap, news_content):
    """
    加入了市值 (market_cap) 上下文
    """
    print(f"   AI正在思考: 【{stock_name}】(市值{market_cap}亿)... ", end="", flush=True)

    # Prompt (RAG)
    system_prompt = f"""
    你是一位资深A股量化分析师。
    当前分析对象：【{stock_name}】 ({stock_code})
    **关键财务数据**：总市值约为 **{market_cap} 亿人民币**。

    任务: 评估新闻对股价的短期 (1-3天)冲击力。
    
    请按以下评估维度进行判断，并给出简短理由:
    1. **量级比对 (关键)**：如果新闻涉及具体金额（如合同、投资），请务必将其与“总市值 {market_cap}亿”进行对比。
       - 例如: 1亿合同对于50亿市值的公司是重大利好(M=3), 但对于5000亿市值的公司只是微风(M=0.1)。
    2. **事件性质**：区分实质性利好（业绩、订单）与情绪性利好（蹭热点、板块跟涨）。
    3. **评分标准**:
       - 强度 (Magnitude, M): 0~5分。
       - 确定性 (Certainty, C): 0~1分 (官方公告=1.0, 传言=0.3)。
       - 方向 (Direction, D): 利好=1, 利空=-1, 中性=0。

    请输出纯 JSON:
    {{
        "reason": "简短理由，必须包含对金额与市值占比的分析（如有）",
        "direction": 1,
        "magnitude": 2.5,
        "certainty": 0.8
    }}
    """

    user_prompt = f"新闻内容：{news_content}"

    try:
        response = client.chat.completions.create(
            model="deepseek-chat",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.1,
            response_format={ "type": "json_object" }
        )

        result_text = response.choices[0].message.content
        data = json.loads(result_text)
        
        final_score = calculate_score(data['direction'], data['magnitude'], data['certainty'])
        
        print(f"(分: {final_score:.2f})")
        data['final_score'] = final_score
        return data

    except Exception as e:
        print(f"分析失败: {e}")
        return {"reason": "Error", "direction": 0, "magnitude": 0, "certainty": 0, "final_score": 0}
