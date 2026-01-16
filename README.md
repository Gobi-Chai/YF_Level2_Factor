
已实现因子

### 1. VOLUME_RATIO
- 上午成交量 / 下午成交量
- 时间窗口：
  - 上午：[09:25, 10:00)
  - 下午：[13:00, 13:30)

### 2. ACT 家族
- ACT / L_ACT / M_ACT / S_ACT / X_ACT
- 基于逐笔成交，按 **分钟 + 原始订单号** 聚合

### 3. ORDER_RATIO 家族
- XLS / L / M / S_ORDER_RATIO
- 基于逐笔委托金额占比

## 三、市场支持

- 深市（SZ）
- 沪市（SH）
- 不同交易所数据通过 loader 层统一 schema


## 四、使用方法

1. 配置 `.env`

```env
DATA_ROOT=/your/level2/data/path
