import qlib
from qlib.config import REG_CN
from qlib.utils import init_instance_by_config
from qlib.workflow import R
from qlib.workflow.record_temp import SignalRecord, PortAnaRecord
import sys
from pathlib import Path

# Config
sys.path.append(str(Path(__file__).resolve().parent.parent))

try:
    from research.custom_handler import MyAlphaHandler
    print("成功加载自定义因子处理器 MyAlphaHandler")
except ImportError as e:
    print(f"加载自定义Handler失败: {e}")
    print("请确认 research/custom_handler.py 文件存在.")
    sys.exit(1)

# Initialize Qlib
provider_uri = str(Path("qlib_data/cn_data").resolve())
qlib.init(provider_uri=provider_uri, region=REG_CN)
print(f"Qlib 初始化完成, 数据源: {provider_uri}")


market = "all"
benchmark = "SH000300"

conf = {
    # Dataset config
    "task": {
        "model": {
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
        },
        "dataset": {
            "class": "DatasetH",
            "module_path": "qlib.data.dataset",
            "kwargs": {
                "handler": {
                    "class": "MyAlphaHandler", 
                    "module_path": "research.custom_handler", 
                    "kwargs": {
                        "start_time": "2020-01-01",
                        "end_time": "2025-12-24",
                        "fit_start_time": "2020-01-01",
                        "fit_end_time": "2022-12-31",
                        "instruments": market,
                        # infer_processors: 数据标准化
                        "infer_processors": [
                             {'class': 'RobustZScoreNorm', 'kwargs': {'fields_group': 'feature', 'clip_outlier': True, 'fit_start_time': '2020-01-01', 'fit_end_time': '2022-12-31'}},
                             {'class': 'Fillna', 'kwargs': {'fields_group': 'feature'}}
                        ],
                        # learn_processors: 训练标签处理
                        "learn_processors": [
                            {'class': 'DropnaLabel'},
                            {'class': 'CSRankNorm', 'kwargs': {'fields_group': 'label'}} 
                        ],
                        # label: 预测目标 (未来5日收益率)
                        "label": ["Ref($close, -5) / $close - 1"] 
                    },
                },
                "segments": {
                    "train": ("2020-01-01", "2023-12-31"), # 训练集
                    "valid": ("2024-01-01", "2024-12-31"), # 验证集
                    "test":  ("2025-01-01", "2025-12-24"), # 测试集
                },
            },
        },
    },
    # 回测记录
    "record": [
        {
            "class": "SignalRecord",
            "module_path": "qlib.workflow.record_temp",
            "kwargs": {
                "model": "<MODEL>",
                "dataset": "<DATASET>",
            },
        },
        {
            "class": "PortAnaRecord",
            "module_path": "qlib.workflow.record_temp",
            "kwargs": {
                "config": {
                    "strategy": {
                        "class": "TopkDropoutStrategy",
                        "module_path": "qlib.contrib.strategy",
                        "kwargs": {
                            "signal": "<PRED>",
                            "topk": 50,
                            "n_drop": 5,
                            "hold_thresh": 1, 
                        },
                    },
                    "backtest": {
                        "start_time": "2025-01-01",
                        "end_time": "2025-12-24",
                        "account": 1000000,
                        "benchmark": benchmark,
                        "exchange_kwargs": {
                            "limit_threshold": 0.095,
                            "deal_price": "close",
                        },
                    },
                },
            },
        },
    ],
}

if __name__ == "__main__":
    with R.start(experiment_name="baseline_custom_factors"):
        print("1. 开始构建数据集以及训练模型 (包含 Sentiment/Sector/Total 因子)...")
        model = init_instance_by_config(conf["task"]["model"])
        dataset = init_instance_by_config(conf["task"]["dataset"])
        model.fit(dataset)

        print("2. 正在生成预测结果...")
        recorder = R.get_recorder()
        sr = SignalRecord(model, dataset, recorder)
        sr.generate()

        print("3. 正在执行回测 (Backtest)...")
        par = PortAnaRecord(recorder, conf["record"][1]["kwargs"]["config"])
        par.generate()

        print(f"\n 回测完成！")
        print(f"结果已保存在: {recorder.get_local_dir()}")
        
        try:
            metrics = recorder.load_object("portfolio_analysis/report_normal_1day.pkl")
            print("\n====== 回测绩效摘要 ======")
            print(metrics)
        except Exception as e:
            print(f"无法直接打印 pickle 报告: {e}")
