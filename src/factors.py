import pandas as pd
import numpy as np

def volume_ratio(trade_df: pd.DataFrame) -> float:
    if trade_df.empty or "hhmm" not in trade_df.columns:
        return float("nan")

    # 每分钟成交量（hhmm 本身就是分钟）
    minute_vol = trade_df.groupby("hhmm")["volume"].sum()

    # 上午 [09:25, 10:00]
    am_vol = minute_vol[(minute_vol.index >= 925) & (minute_vol.index < 1000)].sum()

    # 下午 [13:00, 13:30]
    pm_vol = minute_vol[(minute_vol.index >= 1300) & (minute_vol.index < 1330)].sum()

    if pm_vol == 0:
        return float("nan")

    return am_vol / pm_vol




"""
def act(trade_df: pd.DataFrame) -> float:
    if trade_df.empty:
        return float("nan")

    df = trade_df.copy()

    # 1. time → minute (HHMM)
    df["minute"] = (df["time"] // 1000) // 100

    # 2. 分钟内，按真实单号聚合成交额
    # 买单：minute + buy_id
    buy_amt = (
        df[df["bs_flag"] == "B"]
        .groupby(["minute", "buy_id"])["amount"]
        .sum()
        .groupby("minute")
        .sum()
    )

    # 卖单：minute + sell_id
    sell_amt = (
        df[df["bs_flag"] == "S"]
        .groupby(["minute", "sell_id"])["amount"]
        .sum()
        .groupby("minute")
        .sum()
    )

    # 3. 对齐分钟
    minute_df = pd.concat(
        [buy_amt, sell_amt],
        axis=1,
        keys=["buy_amt", "sell_amt"]
    ).fillna(0)

    # 4. 分钟 ACT
    minute_df["act"] = (
        (minute_df["buy_amt"] - minute_df["sell_amt"]) /
        (minute_df["buy_amt"] + minute_df["sell_amt"])
    )

    minute_df = minute_df.replace([float("inf"), -float("inf")], float("nan")).dropna()

    if minute_df.empty:
        return float("nan")

    # 5. 日内等权平均
    return minute_df["act"].mean()

# 成交额分桶（单位：元）
S_MAX = 4e4
M_MIN, M_MAX = 4e4, 2e5
L_MIN, L_MAX = 2e5, 1e6
X_MIN = 1e6

def _act_by_amount_range(trade_df: pd.DataFrame, low=None, high=None) -> float:
    if trade_df.empty:
        return float("nan")

    df = trade_df.copy()

    # 成交额过滤
    if low is not None:
        df = df[df["amount"] >= low]
    if high is not None:
        df = df[df["amount"] < high]

    if df.empty:
        return float("nan")

    # time → minute
    df["minute"] = (df["time"] // 1000) // 100

    # 买单：minute + buy_id
    buy_amt = (
        df[df["bs_flag"] == "B"]
        .groupby(["minute", "buy_id"])["amount"]
        .sum()
        .groupby("minute")
        .sum()
    )

    # 卖单：minute + sell_id
    sell_amt = (
        df[df["bs_flag"] == "S"]
        .groupby(["minute", "sell_id"])["amount"]
        .sum()
        .groupby("minute")
        .sum()
    )

    minute_df = pd.concat(
        [buy_amt, sell_amt],
        axis=1,
        keys=["buy_amt", "sell_amt"]
    ).fillna(0)

    minute_df["act"] = (
        (minute_df["buy_amt"] - minute_df["sell_amt"]) /
        (minute_df["buy_amt"] + minute_df["sell_amt"])
    )

    minute_df = minute_df.replace([float("inf"), -float("inf")], float("nan")).dropna()

    if minute_df.empty:
        return float("nan")

    return minute_df["act"].mean()

def l_act(trade_df: pd.DataFrame) -> float:
    return _act_by_amount_range(trade_df, low=L_MIN, high=L_MAX)
def m_act(trade_df: pd.DataFrame) -> float:
    return _act_by_amount_range(trade_df, low=M_MIN, high=M_MAX)
def s_act(trade_df: pd.DataFrame) -> float:
    return _act_by_amount_range(trade_df, high=S_MAX)
def x_act(trade_df: pd.DataFrame) -> float:
    return _act_by_amount_range(trade_df, low=X_MIN)
"""

def calc_act_family(trade_df: pd.DataFrame) -> dict:
    """
    计算 ACT / L_ACT / M_ACT / S_ACT / X_ACT
    基于逐笔成交数据（深市）
    """

    df = trade_df.copy()

    # === 1. 构造“分钟 × 单号 × 买卖方向”的成交额 ===
    buy_df = (
        df[df["bs_flag"] == "B"]
        .groupby(["minute", "buy_id"], as_index=False)["amount"]
        .sum()
        .rename(columns={"buy_id": "order_id"})
    )
    buy_df["side"] = "B"

    sell_df = (
        df[df["bs_flag"] == "S"]
        .groupby(["minute", "sell_id"], as_index=False)["amount"]
        .sum()
        .rename(columns={"sell_id": "order_id"})
    )
    sell_df["side"] = "S"

    order_amt = pd.concat([buy_df, sell_df], ignore_index=True)

    # === 2. 按“每分钟内的单号成交额”划分大小单 ===
    def bucket(x):
        if x > 1.0e6:
            return "X"
        elif x >= 2.0e5:
            return "L"
        elif x >= 4.0e4:
            return "M"
        else:
            return "S"

    order_amt["bucket"] = order_amt["amount"].apply(bucket)

    # === 3. 分钟内统计 4 × 2 = 8 个值 ===
    minute_stat = (
        order_amt
        .groupby(["minute", "bucket", "side"])["amount"]
        .sum()
        .reset_index()
    )

    # === 4. 日内加总 ===
    daily = (
        minute_stat
        .groupby(["bucket", "side"])["amount"]
        .sum()
        .unstack(fill_value=0)
    )

    # 防止缺列
    for side in ["B", "S"]:
        if side not in daily.columns:
            daily[side] = 0.0

    # === 5. 计算各类 ACT ===
    def safe_div(num, den):
        return num / den if den != 0 else 0.0

    # 中 + 大 用于 ACT
    buy_LM  = daily.loc[daily.index.isin(["L", "M"]), "B"].sum()
    sell_LM = daily.loc[daily.index.isin(["L", "M"]), "S"].sum()

    act = safe_div(buy_LM - sell_LM, buy_LM + sell_LM)

    res = {
        "ACT": act,
        "L_ACT": safe_div(
            daily.loc["L", "B"] - daily.loc["L", "S"],
            daily.loc["L", "B"] + daily.loc["L", "S"]
        ) if "L" in daily.index else 0.0,
        "M_ACT": safe_div(
            daily.loc["M", "B"] - daily.loc["M", "S"],
            daily.loc["M", "B"] + daily.loc["M", "S"]
        ) if "M" in daily.index else 0.0,
        "S_ACT": safe_div(
            daily.loc["S", "B"] - daily.loc["S", "S"],
            daily.loc["S", "B"] + daily.loc["S", "S"]
        ) if "S" in daily.index else 0.0,
        "X_ACT": safe_div(
            daily.loc["X", "B"] - daily.loc["X", "S"],
            daily.loc["X", "B"] + daily.loc["X", "S"]
        ) if "X" in daily.index else 0.0,
    }

    return res


def calc_order_ratio_family(order_df: pd.DataFrame) -> dict:
    """
    计算：
    - XLS_ORDER_RATIO
    - L_ORDER_RATIO
    - M_ORDER_RATIO
    - S_ORDER_RATIO

    输入：
    - load_order_sz 输出的 DataFrame
      必须包含：minute, order_id, amount
    """

    df = order_df.copy()

    # === 1. 每分钟 × 单号 的委托额 ===
    #（深市逐笔委托：一行就是一个单号，这里 groupby 是为了口径安全）
    order_amt = (
        df.groupby(["minute", "order_id"], as_index=False)["amount"]
        .sum()
    )

    # === 2. bucket 划分（基于“分钟内单号委托额”） ===
    def bucket(x):
        if x > 1.0e6:
            return "X"
        elif x >= 2.0e5:
            return "L"
        elif x >= 4.0e4:
            return "M"
        else:
            return "S"

    order_amt["bucket"] = order_amt["amount"].apply(bucket)

    # === 3. 分钟内统计 4 个 bucket 的委托额 ===
    minute_stat = (
        order_amt
        .groupby(["minute", "bucket"])["amount"]
        .sum()
        .reset_index()
    )

    # === 4. 日内加总 ===
    daily = (
        minute_stat
        .groupby("bucket")["amount"]
        .sum()
    )

    total_amt = daily.sum()

    def safe_div(num, den):
        return num / den if den != 0 else 0.0

    # === 5. 计算各类 ORDER_RATIO ===
    res = {
        "XLS_ORDER_RATIO": safe_div(daily.get("X", 0.0), total_amt),
        "L_ORDER_RATIO":   safe_div(daily.get("L", 0.0), total_amt),
        "M_ORDER_RATIO":   safe_div(daily.get("M", 0.0), total_amt),
        "S_ORDER_RATIO":   safe_div(daily.get("S", 0.0), total_amt),
    }

    return res
