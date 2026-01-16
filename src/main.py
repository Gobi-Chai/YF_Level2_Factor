import os
import pandas as pd
from dotenv import load_dotenv

from .loader import (
    load_trade_sz,
    load_order_sz,
    load_trade_sh,
    load_order_sh,
)

from .factors import (
    volume_ratio,
    calc_act_family,
    calc_order_ratio_family,
)


def main():
    
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    load_dotenv(os.path.join(project_root, ".env"))

    data_root = os.getenv("DATA_ROOT")
    if data_root is None:
        raise RuntimeError("DATA_ROOT 未在 .env 中定义")

    factor_path = os.path.join(project_root, "factorValue_YF.csv")

    
    factor_df = pd.read_csv(factor_path, index_col=0)

    
    for symbol in factor_df.index:
        try:
            print(f">>> 处理股票: {symbol}")

            trade_path = os.path.join(data_root, symbol, "逐笔成交.csv")
            order_path = os.path.join(data_root, symbol, "逐笔委托.csv")

            if not os.path.exists(trade_path) or not os.path.exists(order_path):
                print(f"[SKIP] 数据缺失: {symbol}")
                continue

            # -------- SZ / SH 分流 --------
            if symbol.endswith(".SZ"):
                trade_df = load_trade_sz(trade_path)
                order_df = load_order_sz(order_path)
            elif symbol.endswith(".SH"):
                trade_df = load_trade_sh(trade_path)
                order_df = load_order_sh(order_path)
            else:
                print(f"[SKIP] 未知市场: {symbol}")
                continue

            # =========================
            # 4. 计算因子
            # =========================
            vr = volume_ratio(trade_df)

            act_res = calc_act_family(trade_df)
            order_res = calc_order_ratio_family(order_df)

            # 合并所有结果
            res = {
                "VOLUME_RATIO": vr,
                **act_res,
                **order_res,
            }

            # =========================
            # 5. 回填 factorValue_YF
            # =========================
            for k, v in res.items():
                factor_df.loc[symbol, k] = v

        except Exception as e:
            print(f"[ERROR] {symbol}: {e}")

    # =========================
    # 6. 写回 CSV（一次）
    # =========================
    factor_df.to_csv(factor_path, float_format="%.10f")
    print("===================================")
    print(">>> SZ + SH 所有因子已写入完成")
    print(f">>> 输出文件: {factor_path}")
    print("===================================")


if __name__ == "__main__":
    main()
