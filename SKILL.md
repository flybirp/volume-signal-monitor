---
name: 量能异动监控
description: 全A股「巨量阳K·低位」监控 v4.0。在近30个交易日内寻找同时满足三条件的个股：①巨量阳K且实体较长（当日量>前20日均量×2 且 收盘>开盘×1.04）②历史极量（当日量>300日成交量95分位）③低位（当日收盘<300日收盘价55分位）。输出8列表格+近一年价格K线+成交额副图。核心含义：低位放巨量阳线=主力吸筹/底部启动的前置雷达信号。
agent_created: true
---

# 量能异动监控 v4.0 —— 巨量阳K·低位

## When to use
- 用户要求「全市场扫巨量阳K低位」选股
- 用户提到"巨量阳K"、"低位放巨量"、"倍量阳线"、"底部放量"、"300日95分位"、"缩量走势图"
- 用户要求找「低位主力吸筹」候选股
- 自动化任务每日收盘后扫全市场

## 核心逻辑

**低位放巨量阳线 = 主力吸筹/底部启动的前置雷达信号。**

三条件 **AND** 同时成立（在**监控日**近 30 个交易日内，取最近一个满足的「发生日」）：

1. **巨量阳K且实体较长**：当日 `volume > 前20日均量 × 2` 且 `收盘价 > 开盘价 × 1.04`
2. **历史极量**：该日的 `volume > 近300日成交量 95分位`
3. **低位**：该日的 `收盘价 < 近300日收盘价 55分位`

命中后按第 3 条再细分三档低位：
- 极深：收盘 < 300日收盘价 15分位
- 深度：收盘 < 35分位（且 ≥15分位）
- 低位：收盘 < 55分位（且 ≥35分位）

## 监控日（默认系统时间，可自定义）

- **默认**：监控日 = 系统当天（`datetime.now()`），即「截至今天」扫信号。
- **自定义**：`--date YYYYMMDD`（或 `YYYY-MM-DD`），如 `--date 20240924` 表示「截至 2024-09-24」扫信号。
- 数据拉取自动对齐监控日：tushare 拉截至该日的最近 360 个交易日；westock 按 `--start/--end` 日期范围拉。
- 缓存按监控日命名/匹配：`cache/market_daily_YYYYMMDD.parquet`；查历史监控日时若缓存日期偏早会提示 `--refresh`。
- **注意**：自定义历史监控日时，`scan` 必须配合 `--refresh`（否则会误用今天的缓存，或找不到 ≤ 监控日的缓存直接触发重拉）。

## 输出（8 列表格）

| 列 | 说明 |
| --- | --- |
| A股代码 | tushare ts_code（如 300866.SZ） |
| A股名称 | tushare stock_basic |
| 主营业务 | westock profile.business |
| 绑定度最高概念 | westock industry-chain 关联度最高的节点（1-10分） |
| 发生日交易额 | tushare daily.amount（千元→亿/万） |
| 量能/95分位比值 | 当日量 ÷ 300日95分位（越大越极端） |
| 近一年价格走势+成交额图 | 主图：250日 K 线（涨红跌绿，发生日橙色高亮）+ 副图：成交额 = 量(手)×100×(开盘+收盘)/2，柱状 |
| 低位档 | 极深/深度/低位（相对15/35/55分位） |

## 数据源分工

| 能力 | 数据源 | 说明 |
| --- | --- | --- |
| 全市场日线底座 | tushare pro | 唯一支持全市场批量日线的源，360交易日≈30~90秒 |
| 主营业务 + 概念绑定度 | westock | profile.business + industry-chain 关联度 |
| 交易额 | tushare | daily.amount，单位千元 |

- 覆盖：沪主板(60xxxx) + 科创板(688xxx) + 深主板(00xxxx) + 创业板(30xxxx)

## 安装与配置

### 依赖
```bash
pip install tushare pandas numpy matplotlib
```

### tushare token（必需）
token 通过以下任一方式注入（脚本**不内置** token）：
1. 环境变量：`export TUSHARE_TOKEN="你的token"`
2. 文件：写入 `~/.tushare_token`（内容为 token 字符串，一行）

未配置时运行会报错提示。tushare token 在 https://tushare.pro 注册获取（日线接口需一定积分）。

### westock 命令
`check` / 补字段依赖 westock CLI；脚本自动定位 `~/.local/bin/westock`，或 `export PATH="$HOME/.local/bin:$PATH"`。

### 复权口径（重要）
- **价格统一用「前复权」**，与看盘软件默认一致（用户明确要求）。
- tushare `daily` 接口是不复权原始价；脚本额外拉 `adj_factor` 全市场复权因子，本地算前复权：
  `前复权价 = 原始价 × 复权因子 / 该股最新复权因子`
- 成交量 vol、成交额 amount **不复权**（真实发生值，不受除权影响）。
- 单股 `check` 用 westock `kline --fq qfq`，口径一致。
- **教训**：不复权数据在除权日会产生「假跳空」（如 300109 新开源 20260623 高送转，不复权显示 -16.93%，前复权实为 +0.19%），会污染「低位」判断，必须前复权。

## Steps

### 1. 全市场扫描（主命令）
```bash
cd ~/.workbuddy/skills/量能异动监控
python3 scripts/market_volume_signal.py scan --refresh
# 自定义监控日（截至 2024-09-24）
python3 scripts/market_volume_signal.py scan --refresh --date 20240924
```
- 输出命中股票走势图到 `output/{代码}_{发生日}.png`，控制台打印 8 列表格
- `--refresh`：强制重新拉取 tushare 全市场数据（每日首次运行 / 查历史监控日均需加）
- `--date YYYYMMDD`：监控日，默认=系统今天

### 2. 调试用小样本
```bash
... python3 scripts/market_volume_signal.py scan --limit 300          # 只扫前300只
... python3 scripts/market_volume_signal.py scan --no-enrich          # 不调westock补字段（快）
... python3 scripts/market_volume_signal.py scan --max-enrich 20      # 最多补20只westock字段
... python3 scripts/market_volume_signal.py scan --top 20             # 只保留量能比值最高20只
```

### 3. 单股诊断
```bash
... python3 scripts/market_volume_signal.py check sz300866
... python3 scripts/market_volume_signal.py check sz300866 --date 20240924   # 查历史监控日
```

## 缓存
- 全市场日线缓存：`~/.workbuddy/skills/量能异动监控/cache/market_daily_YYYYMMDD.parquet`
- 走势图输出：`~/.workbuddy/skills/量能异动监控/output/`
- 缓存日期早于今日时会提示 `--refresh`

## 关键参数（scripts/market_volume_signal.py 顶部常量）
```
SCAN_LOOKBACK = 360    # tushare 拉取交易日数
PCT_WINDOW    = 300    # 300日分位窗口
SIGNAL_DAYS   = 30     # 近30日扫描
VOL_PCT       = 95     # 量能95分位
CLOSE_PCT     = 55     # 低位门槛55分位
DOUBLE_VOL    = 2.0    # 巨量倍数
BODY_RATIO    = 1.04   # 实体较长
VOL_MA        = 20     # 前20日均量
MIN_HISTORY   = 320    # 最低历史数据要求
CHART_DAYS    = 250    # 走势图展示天数
```

## Pitfalls
- **运行环境**：需 `python3` 含 tushare/pandas/numpy/matplotlib；系统 python3 缺依赖时先 `pip install -r requirements.txt`
- **westock 命令**：需 `export PATH="$HOME/.local/bin:$PATH"` 或脚本内 `WESTOCK_BIN` 自动定位 `~/.local/bin/westock`
- westock `kline` 收盘价列名是 `last`（不是 `close`）；`amount` 单位是**元**（tushare 是千元，脚本 check 路径已做换算）
- tushare 成交量单位是手（100股）
- tushare 限速 0.15s/次，360交易日约 60~90 秒
- tushare 无可用「概念」接口（concept/concept_detail/ths_index 均无权限或报错），概念绑定度改用 westock `industry-chain stock`

## 旧版说明（v3.1，保留）
- 原 `scripts/volume_monitor.py` 的 watchlist / 横盘缩量（sideway）监控逻辑仍保留，命令不变
- 本 v4.0 为独立精简脚本 `scripts/market_volume_signal.py`，专注于全市场「巨量阳K·低位」选股
