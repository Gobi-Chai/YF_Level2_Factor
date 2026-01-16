
import pandas as pd


PRICE_SCALE = 10000

def load_trade_sz(path: str) -> pd.DataFrame:
    """
    深市逐笔成交数据（用于 ACT / VOLUME_RATIO）
    口径：
    - 仅保留真实成交
    - 保留买卖方向
    - 明确 minute × order_id 粒度
    """

    df = pd.read_csv(path, encoding="gbk")

    # === 1. redefine ===
    df = df.rename(columns={
        df.columns[3]:  "time",        # 成交时间
        df.columns[5]:  "trade_type",  # 成交代码
        df.columns[7]:  "bs_flag",     # B / S
        df.columns[8]:  "price",       # 成交价 (×10000)
        df.columns[9]:  "volume",      # 成交量
        df.columns[10]: "sell_id",     # 叫卖序号
        df.columns[11]: "buy_id",      # 叫买序号
    })

    # === 2. filter ===
    df = df[df["trade_type"] == "0"].copy()

    # === 3. 时间 → minute ===
    df["hhmm"] = (df["time"] // 100000).astype(int)
    df["time"] = pd.to_datetime(df["time"])
    df["minute"] = df["time"].dt.floor("min")
    
    # === 4. 成交额 ===
    df["price"] = df["price"] / 10000.0
    df["amount"] = df["price"] * df["volume"]

    # === 5. 保留“原始委托号”（不合并、不判断归属）===
    # 在 factor 中：
    #   - 买方向用 buy_id
    #   - 卖方向用 sell_id
    # 才能做到“每分钟按单号”

    return df[[
        "minute",
        "hhmm", 
        "bs_flag",
        "buy_id",
        "sell_id",
        "volume",
        "price",
        "amount",
    ]]

def load_order_sz(path:str) -> pd.DataFrame:
    """
    逐笔委托.csv (Order Ratio)
    """
    df = pd.read_csv(path, encoding = "gbk")

    df = df.rename(columns={
        df.columns[3]: "time",       # 委托时间
        df.columns[5]: "order_id",   # 委托号
        df.columns[7]: "side",       # B / S
        df.columns[8]: "price",      # 委托价格 ×10000
        df.columns[9]: "volume",     # 委托数量
    })

    # === 时间 → minute（必须） ===
    df["time"] = pd.to_datetime(df["time"])
    df["minute"] = df["time"].dt.floor("min")
    # === 委托额 ===
    df["price"] = df["price"] / PRICE_SCALE
    df["amount"] = df["price"] * df["volume"]

    return df[[
        "minute",
        "order_id",
        "volume",
        "price",
        "amount",
    ]]


def load_trade_sh(path: str) -> pd.DataFrame:
    """
    沪市逐笔成交（用于 ACT 因子）
    输出 schema 必须与 load_trade_sz 完全一致：
    minute | bs_flag | buy_id | sell_id | volume | price | amount
    """
    df = pd.read_csv(path, encoding="gbk")

    df = df.rename(columns={
        df.columns[3]: "time",      # 时间 HHMMSSmmm
        df.columns[7]: "bs_flag",   # B / S
        df.columns[11]: "buy_id",   # 叫买序号
        df.columns[10]: "sell_id",  # 叫卖序号
        df.columns[8]: "price",     # 成交价格 ×10000
        df.columns[9]: "volume",    # 成交数量
    })

    # ===== 时间 → minute =====
    df["hhmm"] = (df["time"] // 100000).astype(int)
    df["time"] = pd.to_datetime(df["time"], format="%H%M%S%f", errors="coerce")
    df = df.dropna(subset=["time"])
    df["minute"] = df["time"].dt.floor("min")

    # ===== 价格 & 成交额 =====
    df["price"] = df["price"] / PRICE_SCALE
    df["amount"] = df["price"] * df["volume"]

    return df[
        ["minute", "hhmm", "bs_flag", "buy_id", "sell_id", "volume", "price", "amount"]
    ]


def load_order_sh(path: str) -> pd.DataFrame:
    """
    沪市逐笔委托（用于 ORDER_RATIO）
    输出 schema 必须与 load_order_sz 完全一致：
    minute | order_id | volume | price | amount
    """
    df = pd.read_csv(path, encoding="gbk")

    # ===== 列重命名（基于已确认的 column index）=====
    df = df.rename(columns={
        df.columns[3]: "time",       # 时间 HHMMSSmmm
        df.columns[5]: "order_id",   # 交易所委托号
        df.columns[6]: "order_type", # 委托类型 A / D / S
        df.columns[8]: "price",      # 委托价格 ×10000
        df.columns[9]: "volume",     # 委托数量
    })

    # ===== 1. 只保留新增委托（A）=====
    df = df[df["order_type"] == "A"]

    # ===== 2. 时间 → minute =====
    df["time"] = pd.to_datetime(df["time"], format="%H%M%S%f", errors="coerce")
    df = df.dropna(subset=["time"])
    df["minute"] = df["time"].dt.floor("min")

    # ===== 3. 价格 & 委托额 =====
    df["price"] = df["price"] / PRICE_SCALE
    df["amount"] = df["price"] * df["volume"]

    return df[
        ["minute", "order_id", "volume", "price", "amount"]
    ]
