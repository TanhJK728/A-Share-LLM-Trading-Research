import pandas as pd
import json
import os
import akshare as ak
from datetime import datetime
import math

# Config
POSITION_FILE = "trade/positions.json" # 持仓存档文件
SCORE_FILE = "trade/daily_scores.csv" # AI预测结果文件
MAX_POSITIONS = 10 # 最大持仓只数
SINGLE_POSITION_CASH = 20000 # 单只股票拟投入资金 (元)

# 因子参数, 我自己观察到的规律
TARGET_TURNOVER_MIN = 3.0 # 最小换手率 3%
TARGET_TURNOVER_MAX = 12.0 # 最大换手率 12% (太高可能是出货)
TARGET_AMPLITUDE_MIN = 4.0 # 最小振幅 4% (波动太小说明没主力)
WASHOUT_BONUS = 0.2 # 符合洗盘特征的，AI分数额外加 0.2 分 (相当于插队)

def load_positions():
    """加载持仓数据"""
    if os.path.exists(POSITION_FILE):
        with open(POSITION_FILE, 'r') as f:
            return json.load(f)
    return {}

def save_positions(positions):
    """保存持仓数据"""
    with open(POSITION_FILE, 'w') as f:
        json.dump(positions, f, indent=4)

def get_market_snapshot():
    """
    获取全市场实时行情 (含换手、振幅、量比)
    """
    print("正在从 AkShare 获取全市场实时行情 (SnapShot)...")
    try:
        df = ak.stock_zh_a_spot_em()
        
        col_map = {
            '代码': 'code', 'symbol': 'code',
            '最新价': 'price', 'trade': 'price',
            '涨跌幅': 'pct_chg', 'changepct': 'pct_chg',
            '名称': 'name', 'name': 'name',
            '换手率': 'turnover', 'turnoverratio': 'turnover',
            '振幅': 'amplitude', 'amplitude': 'amplitude',
            '量比': 'volume_ratio', 'volumeratio': 'volume_ratio'
        }
        df = df.rename(columns=col_map)
        
        if 'code' not in df.columns or 'price' not in df.columns:
            print("数据列名异常")
            return {}

        df['code'] = df['code'].astype(str).str.zfill(6)
        
        market_map = {}
        for _, row in df.iterrows():
            try:
                raw_price = row['price']
                if raw_price in ['-', '', None]: continue
                price = float(raw_price)
                if math.isnan(price) or price <= 0: continue
                
                # 提取因子数据
                pct = float(row.get('pct_chg', 0)) if not math.isnan(float(row.get('pct_chg', 0))) else 0.0
                turnover = float(row.get('turnover', 0)) if not math.isnan(float(row.get('turnover', 0))) else 0.0
                amplitude = float(row.get('amplitude', 0)) if not math.isnan(float(row.get('amplitude', 0))) else 0.0
                vol_ratio = float(row.get('volume_ratio', 0)) if not math.isnan(float(row.get('volume_ratio', 0))) else 0.0
                
                market_map[row['code']] = {
                    'price': price, 
                    'pct_chg': pct, 
                    'name': str(row.get('name', 'Unknown')),
                    # 因子包
                    'turnover': turnover,
                    'amplitude': amplitude,
                    'volume_ratio': vol_ratio
                }
            except:
                continue
        print(f"行情获取成功, 包含 {len(market_map)} 只有效股票数据")
        return market_map
    except Exception as e:
        print(f"行情获取失败: {e}")
        return {}

def is_limit_up(stock_code, pct_chg, name):
    """判断是否涨停"""
    try:
        if math.isnan(pct_chg): return False
        if stock_code.startswith(('8', '43', '92')): return pct_chg > 29.0
        if stock_code.startswith(('300', '688')): return pct_chg > 19.5
        if 'ST' in name.upper(): return pct_chg > 4.8
        return pct_chg > 9.8
    except:
        return False

def calculate_washout_score(ai_score, market_info):
    """
    计算综合得分: AI预测 + 洗盘特征奖励
    """
    final_score = ai_score
    reasons = []
    
    # 1. 换手率因子 (3% - 12%)
    to = market_info['turnover']
    if TARGET_TURNOVER_MIN <= to <= TARGET_TURNOVER_MAX:
        final_score += WASHOUT_BONUS
        reasons.append(f"换手适中({to}%)")
        
    # 2. 振幅因子 (> 4%)
    amp = market_info['amplitude']
    if amp >= TARGET_AMPLITUDE_MIN:
        final_score += WASHOUT_BONUS
        reasons.append(f"波动活跃({amp}%)")
        
    # 3. 量比因子 (> 1.0 说明今天比过去5天平均量大)
    vr = market_info['volume_ratio']
    if vr > 1.2:
        final_score += 0.1 # 小奖励
        reasons.append(f"放量({vr})")
        
    return final_score, reasons

def run_trading_logic():
    print(f"启动交易扫描 {datetime.now()}")
    
    positions = load_positions()
    if not os.path.exists(SCORE_FILE):
        print("找不到预测文件")
        return
    
    # 读取 AI 预测
    try:
        df_pred = pd.read_csv(SCORE_FILE, index_col=1) 
        df_pred.index = df_pred.index.astype(str).str.zfill(6)
        ai_scores = df_pred['score'].to_dict()
    except:
        print("读取预测文件失败")
        return

    market_data = get_market_snapshot()
    if not market_data: return

    # 1. 持仓管理 & 卖出逻辑
    print("\n" + "="*60)
    print("持仓个股扫描")
    print("="*60)

    total_market_value = 0.0
    total_cost = 0.0
    total_profit = 0.0

    for stock in list(positions.keys()):
        info = positions[stock]
        stock_data = market_data.get(stock)
        
        cost = info['cost']
        shares = info.get('shares', 0)
        
        if stock_data:
            current_price = stock_data['price']
            stock_name = stock_data['name']
        else:
            current_price = cost
            stock_name = "未知"

        max_price = info.get('max_price', cost)
        if current_price > max_price:
            positions[stock]['max_price'] = current_price
            max_price = current_price

        # 收益统计
        market_val = current_price * shares
        profit = (current_price - cost) * shares
        profit_pct = (current_price - cost) / cost

        total_market_value += market_val
        total_cost += (cost * shares)
        total_profit += profit

        # 决策
        score = ai_scores.get(stock, -999)
        drawdown = (max_price - current_price) / max_price if max_price > 0 else 0
        
        action = "HOLD"
        reason = ""
        
        if profit_pct < -0.05:
            action = "SELL"
            reason = "止损"
        elif profit_pct > 0.05 and drawdown > 0.03:
            action = "SELL"
            reason = "移动止盈"
        elif score < 0:
            action = "SELL"
            reason = f"AI看空({score:.2f})"

        status = "LOSS" if profit < 0 else "PROFIT"
        print(f"{status} {stock} {stock_name:<6} | 盈亏: {profit:>8.2f} ({profit_pct:>6.2%}) | AI: {score:>5.2f} | {action} {reason}")

        if action == "SELL":
            print(f"   执行卖出: {stock} @ {current_price}")
            del positions[stock]

    # 2. 智能选股 (AI + 洗盘因子)
    slots_available = MAX_POSITIONS - len(positions)
    if slots_available > 0:
        print("\n" + "="*60)
        print("智能选股 (AI预测 + 游资洗盘特征)")
        print("="*60)
        
        # 1. 预筛选：只看 AI Top 50
        candidates = []
        for stock, ai_score in ai_scores.items():
            if stock in positions: continue # 已持仓跳过
            if ai_score < 0.1: continue     # AI 分数太低直接不要
            
            stock_data = market_data.get(stock)
            if not stock_data: continue
            
            # 计算综合得分
            final_score, reasons = calculate_washout_score(ai_score, stock_data)
            
            candidates.append({
                "code": stock,
                "name": stock_data['name'],
                "price": stock_data['price'],
                "pct": stock_data['pct_chg'],
                "ai_score": ai_score,
                "final_score": final_score,
                "reasons": reasons,
                "is_limit_up": is_limit_up(stock, stock_data['pct_chg'], stock_data['name'])
            })
            
        # 2. 按综合得分排序 (让符合洗盘特征的股票排前面)
        candidates.sort(key=lambda x: x['final_score'], reverse=True)
        
        # 3. 选出 Top N
        for cand in candidates:
            if slots_available <= 0: break
            
            if cand['is_limit_up']:
                print(f"跳过 {cand['code']} {cand['name']}: 已涨停 (+{cand['pct']}%)")
                continue
                
            shares = math.floor(SINGLE_POSITION_CASH / cand['price'] / 100) * 100
            if shares < 100: continue
            
            # 选中理由
            bonus_str = " + ".join(cand['reasons']) if cand['reasons'] else "纯AI推荐"
            print(f"拟买入 {cand['code']} {cand['name']}: 现价{cand['price']} | 综合分 {cand['final_score']:.4f} (AI {cand['ai_score']:.2f})")
            print(f"   理由: {bonus_str}")
            
            positions[cand['code']] = {
                "cost": cand['price'],
                "shares": shares,
                "max_price": cand['price'],
                "buy_date": datetime.now().strftime("%Y-%m-%d")
            }
            slots_available -= 1

    save_positions(positions)

    # 看板
    total_return_pct = (total_profit / total_cost) if total_cost > 0 else 0.0
    print("\n" + "="*60)
    print(f"账户看板 | 市值: {total_market_value:,.0f} | 浮盈: {total_profit:+,.0f} ({total_return_pct:+.2%})")
    print("="*60 + "\n")

if __name__ == "__main__":
    run_trading_logic()
