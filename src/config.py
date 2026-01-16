# src/config.py

# ========== 时间窗口 ==========
MORNING_START = "09:25:00"
MORNING_END   = "10:00:00"

AFTERNOON_START = "13:00:00"
AFTERNOON_END   = "13:30:00"

#  订单金额分层（单位：元） 
X_ORDER = 1_000_000      # 超大单
L_ORDER = 200_000        # 大单
M_ORDER = 40_000         # 中单

# ========== 基础设置 ==========
PRICE_SCALE = 10000      # 价格缩放
