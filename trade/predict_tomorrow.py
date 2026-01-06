import qlib
from qlib.utils import init_instance_by_config
import pandas as pd
import sys
from pathlib import Path
from datetime import datetime, timedelta

# 引入自定义 Handler
sys.path.append(str(Path(__file__).resolve().parent.parent))
from research.custom_handler import MyAlphaHandler

# Config
QLIB_DATA_DIR = str(Path("qlib_data/cn_data").resolve())

def get_auto_date():
    """
    智能获取基准日期：
    1. 如果今天是周一到周五(0-4)，返回今天。
    2. 如果今天是周六(5)，返回昨天(周五)。
    3. 如果今天是周日(6)，返回前天(周五)。
    这样保证周末跑脚本也不会报错，周一跑就是最新的。
    """
    now = datetime.now()
    weekday = now.weekday() # 0=周一, 6=周日
    
    if weekday == 5: # 周六
        target = now - timedelta(days=1)
    elif weekday == 6: # 周日
        target = now - timedelta(days=2)
    else: # 周一到周五
        target = now
        
    return target.strftime("%Y-%m-%d")

TARGET_DATE = get_auto_date()

def build_dataset(date_str, is_train=True):
    # 训练集和推理集的配置分开
    if is_train:
        # 训练集：从 2020-01-01 到 TARGET_DATE
        segments = {"train": ("2020-01-01", date_str)}
        learn_processors = [{"class": "CSRankNorm", "kwargs": {"fields_group": "label"}}]
        label = ["Ref($close, -5) / $close - 1"]
    else:
        # 推理集：只取 TARGET_DATE 这一天
        # 推理时绝对不能用 DropnaLabel，否则最后一天会被删
        segments = {"test": (date_str, date_str)}
        learn_processors = [] 
        label = ["Ref($close, -5) / $close - 1"] # 占位符

    ds_conf = {
        "class": "DatasetH",
        "module_path": "qlib.data.dataset",
        "kwargs": {
            "handler": {
                "class": "MyAlphaHandler",
                "module_path": "research.custom_handler",
                "kwargs": {
                    "start_time": segments.get("train", segments.get("test"))[0],
                    "end_time": segments.get("train", segments.get("test"))[1],
                    "fit_start_time": "2020-01-01",
                    "fit_end_time": date_str,
                    "instruments": "all",
                    "infer_processors": [{"class": "Fillna", "kwargs": {"fields_group": "feature"}}],
                    "learn_processors": learn_processors,
                    "label": label,
                },
            },
            "segments": segments,
        },
    }
    return init_instance_by_config(ds_conf)

def predict():
    qlib.init(provider_uri=QLIB_DATA_DIR, region="cn")

    print(f"自动基准日: {TARGET_DATE} (系统时间: {datetime.now().strftime('%Y-%m-%d')})")
    
    print("1. 正在加载训练数据 (Train)...")
    ds_train = build_dataset(TARGET_DATE, is_train=True)
    
    print("2. 正在加载推理数据 (Infer)...")
    ds_infer = build_dataset(TARGET_DATE, is_train=False)

    # 检查推理数据
    try:
        example_data = ds_infer.prepare("test", col_set="feature")
        if example_data is None or len(example_data) == 0:
            print(f"推理数据集为空! 请检查是否有 {TARGET_DATE} 的数据.")
            return
        print(f"推理集准备就绪，共 {len(example_data)} 只股票待预测。")
    except Exception as e:
        print(f"数据检查警告: {e}")

    # Model configuration
    model_conf = {
        "class": "LGBModel",
        "module_path": "qlib.contrib.model.gbdt",
        "kwargs": {
            "loss": "mse",
            "colsample_bytree": 0.8879,
            "learning_rate": 0.0421,
            "subsample": 0.8789,
            "lambda_l1": 205.6999,
            "lambda_l2": 580.9768,
            "max_depth": 8,
            "num_leaves": 210,
            "num_threads": 20,
        },
    }
    model = init_instance_by_config(model_conf)

    print("3. 开始训练模型...")
    model.fit(ds_train)

    print("4. 生成预测结果...")
    pred = model.predict(ds_infer)

    # 确保是 DataFrame 并且有列名
    if isinstance(pred, pd.Series):
        pred = pred.to_frame("score")
    if isinstance(pred, pd.DataFrame):
        pred.columns = ["score"]

    if isinstance(pred, pd.DataFrame) and not pred.empty:
        if len(pred) == 0:
             print("预测结果 DataFrame 行数为 0。")
             return

        # 过滤负分 并取 Top 20
        pred = pred[pred["score"] > 0] 

        # 过滤掉短期涨幅过大的“妖股”
        # pred = pred[pred['pct_change_20days'] < 0.3] 
    
        top_picks = pred.sort_values(by="score", ascending=False).head(20)

        print("\n" + "=" * 50)
        print(f"AI 选股最终名单 (Top 20)")
        print(f"基准日期: {TARGET_DATE}")
        print("=" * 50)
        print(f"{'代码':<10} {'AI评分 (预测超额收益)':<25}")
        print("-" * 40)

        for inst, row in top_picks.iterrows():
            stock_code = inst[1] if isinstance(inst, tuple) else inst
            score = row["score"]
            print(f"{stock_code:<10} {score:.6f}")

        print("=" * 50)
    else:
        print(f"预测结果格式异常: {type(pred)}")

    output_path = "trade/daily_scores.csv"
    pred.to_csv(output_path)
    print(f"预测结果已保存至: {output_path}")

if __name__ == "__main__":
    predict()
