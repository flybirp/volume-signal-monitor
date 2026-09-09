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

## 数据源

| 能力 | 数据源 |
| --- | --- |
| 全市场日线底座 + 前复权因子 | tushare pro |
| 主营业务 + 概念绑定度 | westock CLI |

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
