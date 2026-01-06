from qlib.contrib.data.handler import Alpha158

class MyAlphaHandler(Alpha158):
    """
    继承 Alpha158, 并追加自定义因子
    """
    def get_feature_config(self):
        conf = super().get_feature_config()

        # 情况A：多数版本 Alpha158 返回 (fields, names)
        if isinstance(conf, (tuple, list)) and len(conf) == 2:
            base_fields, base_names = conf
            fields = list(base_fields)
            names = list(base_names)

            # 追加表达式与名字(一一对应的两个列表）
            fields += ["$sentiment", "$sector_score", "$total_score"]
            names  += ["sentiment",  "sector_score",  "total_score"]

            return (fields, names)

        # 情况B：少数旧版可能返回 dict，且 'feature' 键下是 (fields, names)
        if isinstance(conf, dict):
            f, n = conf.get("feature", ([], []))
            fields = list(f) + ["$sentiment", "$sector_score", "$total_score"]
            names  = list(n) + ["sentiment",  "sector_score",  "total_score"]
            conf["feature"] = (fields, names)
            return conf

        return conf