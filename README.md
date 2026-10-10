# 量能异动监控（巨量阳K·低位）

全 A 股「低位放巨量阳线」监控选股脚本。**低位放巨量阳线 = 主力吸筹 / 底部启动的前置雷达信号。**

## 核心逻辑

三条件 **AND** 同时成立（在监控日近 30 个交易日内取最近一个「发生日」）：

1. **巨量阳K且实体较长**：当日 `volume > 前20日均量 × 2` 且 `收盘价 > 开盘价 × 1.04`
2. **历史极量**：当日 `volume > 近300日成交量 95分位`
3. **低位**：当日 `收盘价 < 近300日收盘价 55分位`

输出 8 列表格 + 近一年价格 K 线 + 成交额副图。数据统一**前复权**口径，与看盘软件默认一致。

## 快速开始

```bash
# 1. 安装依赖
pip install -r requirements.txt

# 2. 配置 tushare token（任选其一）
export TUSHARE_TOKEN="你的token"
# 或写入 ~/.tushare_token（内容为 token 字符串）

# 3. 全市场扫描（监控日 = 系统今天）
python3 scripts/market_volume_signal.py scan --refresh

# tushare 无权限时改用 westock 源（约 25 分钟）
python3 scripts/market_volume_signal.py scan --refresh --source westock

# 自定义监控日（截至 2024-09-24）
python3 scripts/market_volume_signal.py scan --refresh --date 20240924

# 单股诊断
python3 scripts/market_volume_signal.py check sz300866
python3 scripts/market_volume_signal.py check sz300866 --date 20240924
```

## 输出

- 控制台 + CSV：`output/巨量阳K低位_命中列表.csv`（8 列表格）
- HTML 报告：`output/巨量阳K低位_监控报告.html`
- 走势图：`output/{代码}_{发生日}.png`（价格 K 线 + 成交额副图）

## 数据源（三级兜底）

| 能力 | 数据源 |
| --- | --- |
| 全市场日线底座（①→②→③ 自动兜底） | ① tushare pro（60~90 秒，需 token + 接口权限）<br>② 腾讯行情 `web.ifzq.gtimg.cn`（并发快，高频会被 WAF 封）<br>③ westock CLI `kline` 批量（≤20 代码/次，QPM≈10，~25 分钟） |
| 股票清单（代码 + 名称） | westock CLI `screen condition` |
| 主营业务 + 概念绑定度 | westock CLI `profile` / `industry-chain stock` |

用 `--source auto|tushare|tx|westock` 手动指定，默认 `auto`。

> ⚠️ 实测提醒：tushare token 的接口权限可能失效（返回 `40203 没有接口访问权限`），
> 腾讯/东财行情接口连续几千次请求会被封。当前最稳的是 `--source westock`。

## 常用命令

```bash
# 全市场扫描（tushare 不可用时直接指定 westock 源）
python3 scripts/market_volume_signal.py scan --refresh --source westock --date 20261009

# 补漏：只抓缓存里缺失的股票（westock 限流时是整批 20 只一起丢，必做）
python3 scripts/market_volume_signal.py fill --date 20261009 --per-min 6 --workers 2

# 补完再扫一次（不加 --refresh，用缓存）+ 串行补字段
python3 scripts/market_volume_signal.py scan --date 20261009 --workers 1
```

## 数据口径坑（详见 SKILL.md）

- **科创板（688/689）的 `volume` 单位是「股」**，主板 / 创业板是「手」——腾讯行情与 westock 都是这个口径。
  不换算会让成交额虚高 100 倍。
- 价格统一**前复权**；不复权数据在除权日会产生「假跳空」，污染「低位」判断。
- westock 补字段（profile / industry-chain）**并发会返回降级数据**（概念关联度变 0），大批量请用 `--workers 1`。
- westock 产业链覆盖率约 50%，「未找到所属产业链」属正常，不是抓取失败。

## 目录结构

```
量能异动监控/
├── SKILL.md                        # 完整 skill 说明（含参数、复权口径、pitfalls）
├── requirements.txt
├── scripts/
│   ├── market_volume_signal.py     # v4.0 主脚本（巨量阳K·低位全市场扫描）
│   └── volume_monitor.py           # v3.1 旧版（watchlist / 横盘缩量，保留）
├── watchlist.json                  # v3.1 监控列表
└── sideway_watchlist.json          # v3.1 横盘缩量列表
```

详细说明见 [SKILL.md](./SKILL.md)。
