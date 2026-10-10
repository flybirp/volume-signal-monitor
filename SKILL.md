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

## 数据源分工（三级兜底，2026-10 修订）

| 能力 | 数据源 | 说明 |
| --- | --- | --- |
| 全市场日线底座 | ① tushare pro → ② 腾讯行情 → ③ westock 批量 K 线 | `--source auto` 依次兜底；也可手动指定 |
| 股票清单（代码+名称） | westock `screen condition` | 缓存 `cache/universe_YYYYMMDD.csv`（约 5026 只） |
| 主营业务 + 概念绑定度 | westock | profile.business + industry-chain 关联度 |
| 交易额 | tushare amount 千元 / westock amount 元 / 腾讯估算 | 见下方「成交额口径」 |

- 覆盖：沪主板(60xxxx) + 科创板(688xxx/689xxx) + 深主板(00xxxx) + 创业板(30xxxx)
- **`--source` 参数**：`auto`（默认，tushare→腾讯→westock）| `tushare` | `tx` | `westock`

### 三个数据源的实测表现（2026-10-10）

| 源 | 速度 | 限制 | 备注 |
| --- | --- | --- | --- |
| tushare | 60~90 秒 | 需 token + 接口权限 | **2026-10 实测本机 token 无 trade_cal/daily/stock_basic 权限**（报 40203），已自动兜底 |
| 腾讯行情 `web.ifzq.gtimg.cn/appstock/app/fqkline/get` | 5026 只约 15~20 分钟 | 高频会被腾讯 WAF 封（返回跳转页，非 JSON） | 无成交额字段，需估算；失败率约 50%（并发 12 时） |
| westock `kline` 批量 | 5026 只约 25 分钟 | **≤20 代码/次**，QPM≈10，超限返回「状态: error」 | 前复权 + 精确成交额，最可靠但最慢 |

## ★ 成交额口径（必读）

- **科创板（688/689）的 `volume` 单位是「股」**，沪/深主板与创业板是「手」——腾讯行情与 westock **都是**这个口径。
  已换算：脚本内统一把 68 开头的量除以 100 转成「手」，否则成交额会**虚高 100 倍**（曾出现「光峰科技 240亿」这种荒谬值）。
  自检：688084 晶品特装 2026-09-02 成交额 1.69 亿（若显示 169 亿就是没换算）。
- 腾讯源无 amount 字段，按 `股数 × (开+收)/2` 估算，与前复权量价相乘等价于真实成交额，误差约 1~2%（VWAP vs 均价）。
- westock/tushare 源用接口真实 amount（westock 单位元 → 脚本转千元）。

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
# 指定数据源（tushare 失效时手动切 westock，约 25 分钟）
python3 scripts/market_volume_signal.py scan --refresh --source westock --date 20261009
```
- 输出命中股票走势图到 `output/{代码}_{发生日}.png`，控制台打印 8 列表格
- `--refresh`：强制重新拉取全市场数据（每日首次运行 / 查历史监控日均需加）
- `--date YYYYMMDD`：监控日，默认=系统今天
- `--source auto|tushare|tx|westock`：全市场日线数据源，默认 auto（三级兜底）
- **运行环境**：本机系统 python3 无 tushare，用托管 venv：
  `~/.workbuddy/binaries/python/envs/default/bin/python`

### 2. 补漏（westock 源限流整批丢失时必做）
```bash
... python3 scripts/market_volume_signal.py fill --date 20261009 --per-min 6 --workers 2
```
- 只抓「清单里有、缓存里没有」的股票，合并回 `cache/market_daily_YYYYMMDD.parquet`
- 批量抓取被限流时是**整批 20 只一起丢**，第一次跑完覆盖率常只有 77%，补一轮可到 98%
- 补完要**再跑一次 scan（不加 --refresh）**才会重算命中

### 3. 调试用小样本
```bash
... python3 scripts/market_volume_signal.py scan --limit 300          # 只扫前300只
... python3 scripts/market_volume_signal.py scan --no-enrich          # 不调westock补字段（快）
... python3 scripts/market_volume_signal.py scan --max-enrich 20      # 最多补20只westock字段
... python3 scripts/market_volume_signal.py scan --top 20             # 只保留量能比值最高20只
... python3 scripts/market_volume_signal.py scan --workers 1          # 补字段改串行（概念关联度才准，见 Pitfalls）
```

### 4. 单股诊断
```bash
... python3 scripts/market_volume_signal.py check sz300866
... python3 scripts/market_volume_signal.py check sz300866 --date 20240924   # 查历史监控日
```

### 推荐全流程（当前 tushare 不可用时的现实路径，总耗时约 1.5 小时）
```bash
PY=~/.workbuddy/binaries/python/envs/default/bin/python
export PATH="$HOME/.local/bin:$PATH"
$PY scripts/market_volume_signal.py scan --refresh --source westock --no-enrich --date 20261009  # ~30min
$PY scripts/market_volume_signal.py fill --date 20261009 --per-min 6 --workers 2                 # ~10min
$PY scripts/market_volume_signal.py scan --date 20261009 --workers 1                             # ~40min（串行补字段）
```

## 缓存
- 全市场日线缓存：`~/.workbuddy/skills/量能异动监控/cache/market_daily_YYYYMMDD.parquet`
- 走势图输出：`~/.workbuddy/skills/量能异动监控/output/`
- 缓存日期早于今日时会提示 `--refresh`

## 关键参数（scripts/market_volume_signal.py 顶部常量）
```
SCAN_LOOKBACK = 360    # 拉取交易日数（三个数据源共用）
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
- **运行环境**：本机系统 python3 无 tushare/pandas/matplotlib，必须用托管 venv
  `~/.workbuddy/binaries/python/envs/default/bin/python`（已装齐依赖）
- **tushare token 权限会失效**：2026-10 实测本机 `~/.tushare_token` 对 trade_cal/daily/adj_factor/stock_basic
  一律返回 `40203 没有接口访问权限`。此时脚本会自动兜底到腾讯行情/westock；若想恢复 tushare 需换 token 或补积分。
- **腾讯行情会封 IP**：连续几千次请求后返回 WAF 跳转页（非 JSON），`r.json()` 会抛异常。
  脚本已捕获并按空数据处理 → 覆盖率掉到 ~50%，此时必须换源（`--source westock`）。
- **东财 push2his 同样会被封**（2026-10 实测：200 次并发后返回空）。新浪 `CN_MarketData.getKLineData`
  可用但只给**不复权**数据，不能用于「低位」判断（假跳空污染），只能当应急量能参考。
- **科创板量单位是股**（见上方「成交额口径」），不换算会让成交额虚高 100 倍。
- **westock kline 批量上限 20 代码/次**，超限直接报「一次最多查询 20 个代码」；QPM≈10，
  超限时返回「状态: error」——脚本内置限速器（10 批/分钟）+ 退避重试，全市场约 25 分钟。
  限流是**整批 20 只一起丢**，所以跑完必须先 `fill` 再重扫（见 Steps）。
- **★ 补字段不能高并发**：`profile` / `industry-chain stock` 在并发（workers≥4）下 westock 会返回
  **降级数据**——`industry-chain` 的「所属节点」表名还在、但 `correlation` 全为 0，
  表现为 CSV 里「概念(0)」。串行（`--workers 1`）才正常。脚本已对「关联度=0」加退避重试，
  但根因是并发，跑大批量请用 `--workers 1`（实测 295 只串行约 40~78 分钟，限流时 ~16s/只）。
- **westock industry-chain 列名已改英文**：`nodeName` / `category` / `correlation` / `nodeCode`、
  主题表 `themeName` / `themeType`。脚本原来只认中文「节点名称/关联度/主题名称」→ 概念列全空。
  现已中英文双兼容，改动 westock 输出格式时记得同步这里。
- **概念列为空不一定是 bug**：westock 产业链覆盖率约 50%，返回「未找到所属产业链」就是真没有
  （2026-10-09 那批 295 只里 69 只属于这种情况），不要当成抓取失败反复重试。
- **westock 命令**：需 `export PATH="$HOME/.local/bin:$PATH"` 或脚本内 `WESTOCK_BIN` 自动定位 `~/.local/bin/westock`
- westock `kline` 收盘价列名是 `last`（不是 `close`）；`amount` 单位是**元**（tushare 是千元，脚本已换算）
- tushare 成交量单位是手（100股）
- tushare 限速 0.15s/次，360交易日约 60~90 秒
- tushare 无可用「概念」接口（concept/concept_detail/ths_index 均无权限或报错），概念绑定度改用 westock `industry-chain stock`
- **后台运行**：`nohup ... &` 在工具 shell 退出时会被杀，长任务要用工具的 run_in_background 机制

## 旧版说明（v3.1，保留）
- 原 `scripts/volume_monitor.py` 的 watchlist / 横盘缩量（sideway）监控逻辑仍保留，命令不变
- 本 v4.0 为独立精简脚本 `scripts/market_volume_signal.py`，专注于全市场「巨量阳K·低位」选股
