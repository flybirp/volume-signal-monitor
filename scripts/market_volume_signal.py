#!/usr/bin/env python3
"""
全A股「巨量阳K·低位」监控 v4.0

核心逻辑（三条件 AND，近30个交易日内找「发生日」）：
  1. 巨量阳K且实体较长：当日 volume > 前20日均量 × 2 且 收盘 > 开盘 × 1.04
  2. 历史极量：当日 volume > 近300日 volume 的 95 分位
  3. 低位：当日收盘价 < 近300日收盘价的 55 分位
  遍历近30个交易日，取最近一个同时满足 1+2+3 的「发生日」。

输出列（8 列）：
  代码 | 名称 | 主营业务 | 绑定度最高概念 | 发生日交易额 | 量能/95分位比值 | 近一年价格走势+成交额 | 低位档

数据源分工：
  - 全市场日线底座：tushare（批量，30~90 秒，唯一有全市场批量日线能力）
  - 主营业务 + 概念绑定度：westock（profile.business + industry-chain 关联度 1-10）
  - 交易额：tushare daily.amount（单位：千元）

用法:
  python market_volume_signal.py scan                 # 全市场扫描（监控日=系统今天）
  python market_volume_signal.py scan --date 20240924 # 指定监控日（如 2024-09-24）
  python market_volume_signal.py scan --limit 300     # 只扫描前 N 只（调试）
  python market_volume_signal.py scan --no-enrich     # 只扫描，不调 westock 补字段（快速）
  python market_volume_signal.py scan --refresh       # 强制刷新 tushare 缓存
  python market_volume_signal.py check sz300866                # 单股诊断（westock 日线）
  python market_volume_signal.py check sz300866 --date 20240924
"""

import numpy as np
import pandas as pd
import os
import sys
import json
import time
import argparse
import subprocess
import shutil
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

plt.rcParams["font.sans-serif"] = ["PingFang SC", "Hiragino Sans GB", "Arial Unicode MS", "STHeiti"]
plt.rcParams["axes.unicode_minus"] = False

# ============================================================
# 配置
# ============================================================
SKILL_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CACHE_DIR = os.path.join(SKILL_DIR, "cache")
OUT_DIR = os.path.join(SKILL_DIR, "output")
WESTOCK_BIN = shutil.which("westock") or os.path.expanduser("~/.local/bin/westock")

def _load_tushare_token():
    """加载 tushare token：优先环境变量 TUSHARE_TOKEN，其次 ~/.tushare_token 文件。"""
    token = os.environ.get("TUSHARE_TOKEN", "").strip()
    if token:
        return token
    for p in [os.path.expanduser("~/.tushare_token"), os.path.join(SKILL_DIR, ".tushare_token")]:
        if os.path.exists(p):
            with open(p) as f:
                t = f.read().strip()
            if t:
                return t
    return ""

SCAN_LOOKBACK = 360    # tushare 拉取交易日数（300日分位窗口 + 30日扫描 + 余量）
PCT_WINDOW = 300       # 300 日分位窗口
SIGNAL_DAYS = 30       # 近 30 日扫描窗口
VOL_PCT = 95           # 量能 95 分位
CLOSE_PCT = 55         # 低位门槛：55 分位
DOUBLE_VOL = 2.0       # 巨量倍数
BODY_RATIO = 1.04      # 实体较长：收盘 > 开盘 × 1.04
VOL_MA = 20            # 前 20 日均量
MIN_HISTORY = 320      # 最低历史数据要求
CHART_DAYS = 250       # 走势图展示近一年（约 250 交易日）


# ============================================================
# 监控日解析
# ============================================================
def parse_date_arg(d):
    """把监控日参数解析为 datetime。

    支持：None（默认=系统今天）、'20240924'、'2024-09-24'、'2024/09/24'。
    返回 datetime；非法格式抛 ValueError。
    """
    if d is None or str(d).strip() == "":
        return datetime.now()
    s = str(d).strip().replace("-", "").replace("/", "")
    if len(s) != 8 or not s.isdigit():
        raise ValueError(f"监控日格式错误：{d}，应为 YYYYMMDD 或 YYYY-MM-DD")
    return datetime.strptime(s, "%Y%m%d")


def fmt_date_arg(dt):
    """datetime -> 'YYYY-MM-DD'（westock --start/--end 格式）"""
    return dt.strftime("%Y-%m-%d")


# ============================================================
# 数据获取：tushare（全市场批量日线底座）
# ============================================================
def get_tushare_pro():
    import tushare as ts
    token = _load_tushare_token()
    if not token:
        raise RuntimeError(
            "未配置 tushare token。请设置环境变量 TUSHARE_TOKEN，"
            "或写入 ~/.tushare_token 文件（内容为 token 字符串）。"
        )
    return ts.pro_api(token)


def fetch_full_market_daily(lookback=SCAN_LOOKBACK, end_date=None):
    """用 tushare 批量拉取全市场日线 + 复权因子，计算前复权价。返回 DataFrame。

    end_date: 监控日（'YYYYMMDD' 或 None=系统今天）。拉取截至该日的前 lookback 个交易日。
    """
    pro = get_tushare_pro()
    end_dt = parse_date_arg(end_date)
    end_date = end_dt.strftime("%Y%m%d")
    start_date = (end_dt - timedelta(days=lookback * 2)).strftime("%Y%m%d")

    cal = pro.trade_cal(exchange="SSE", start_date=start_date, end_date=end_date, is_open="1")
    trade_dates = sorted(cal["cal_date"].tolist(), reverse=True)[:lookback]

    print(f"拉取全市场日线+复权因子 | 监控日 {end_date} | 数据区间 {trade_dates[-1]}~{trade_dates[0]} | {len(trade_dates)} 个交易日", flush=True)

    all_dfs = []
    all_adj = []
    for i, date in enumerate(trade_dates):
        try:
            df = pro.daily(trade_date=date)
            if df is not None and len(df) > 0:
                all_dfs.append(df)
            adj = pro.adj_factor(trade_date=date)
            if adj is not None and len(adj) > 0:
                all_adj.append(adj[["ts_code", "trade_date", "adj_factor"]])
            if (i + 1) % 20 == 0:
                print(f"  进度: {i + 1}/{len(trade_dates)} | {date}", flush=True)
            time.sleep(0.15)  # tushare 限速
        except Exception as e:
            print(f"  [WARN] {date}: {e}", file=sys.stderr, flush=True)
            time.sleep(0.5)

    if not all_dfs:
        print("❌ 未能获取任何数据")
        return None

    combined = pd.concat(all_dfs, ignore_index=True)
    combined = combined[combined["ts_code"].str.match(r"^(60|68|00|30)\d{4}\.(SH|SZ)$")]

    adj_combined = pd.concat(all_adj, ignore_index=True)
    adj_combined = adj_combined[adj_combined["ts_code"].str.match(r"^(60|68|00|30)\d{4}\.(SH|SZ)$")]

    # 合并日线 + 复权因子，按 ts_code/日期排序
    combined = combined.merge(adj_combined, on=["ts_code", "trade_date"], how="left")
    combined = combined.sort_values(["ts_code", "trade_date"]).reset_index(drop=True)

    # 复权因子缺失时前后填充（停牌/新股初期可能缺）
    combined["adj_factor"] = combined.groupby("ts_code")["adj_factor"].ffill().bfill()
    combined = combined.dropna(subset=["adj_factor"])

    # 前复权：qfq = raw × adj_factor / latest_factor（每股最新复权因子）
    # adj_factor 随除权除息单调递增，group max = 最新因子
    latest = combined.groupby("ts_code")["adj_factor"].transform("max")
    for col in ["open", "high", "low", "close"]:
        combined[col] = combined[col] * combined["adj_factor"] / latest

    print(f"全市场数据: {len(combined)} 条 | {combined['ts_code'].nunique()} 只股票（已前复权）", flush=True)

    os.makedirs(CACHE_DIR, exist_ok=True)
    cache_path = os.path.join(CACHE_DIR, f"market_daily_{end_date}.parquet")
    combined.to_parquet(cache_path, index=False)
    print(f"缓存已保存: {cache_path}", flush=True)
    return combined


def load_cached_market_daily(end_date=None):
    """加载 ≤ 监控日 的最新缓存。找不到时返回 None（调用方会走拉取/刷新）。"""
    os.makedirs(CACHE_DIR, exist_ok=True)
    cache_files = sorted(f for f in os.listdir(CACHE_DIR) if f.startswith("market_daily_") and f.endswith(".parquet"))
    if not cache_files:
        return None
    end_str = parse_date_arg(end_date).strftime("%Y%m%d")
    # 找缓存日期 ≤ 监控日 的最新一个
    valid = [f for f in cache_files
             if f.replace("market_daily_", "").replace(".parquet", "") <= end_str]
    if not valid:
        print(f"⚠️ 无 ≤ {end_str} 的缓存，需 --refresh 重新拉取", flush=True)
        return None
    latest = os.path.join(CACHE_DIR, valid[-1])
    df = pd.read_parquet(latest)
    cache_date = valid[-1].replace("market_daily_", "").replace(".parquet", "")
    if cache_date < end_str:
        print(f"⚠️ 缓存日期 {cache_date} 早于监控日 {end_str}，建议 --refresh", flush=True)
    return df


# ============================================================
# 数据获取：westock（主营业务 + 概念绑定度）
# ============================================================
def ts_to_westock(ts_code):
    """600519.SH -> sh600519"""
    num, ex = ts_code.split(".")
    return ex.lower() + num


def _run_westock(args, timeout=30):
    try:
        r = subprocess.run([WESTOCK_BIN] + args, capture_output=True, text=True, timeout=timeout)
        return r.stdout if r.returncode == 0 else ""
    except Exception as e:
        print(f"  [WARN] westock {' '.join(args)}: {e}", file=sys.stderr, flush=True)
        return ""


def get_profile(code):
    """westock profile -> (名称 name, 主营业务 business, 行业 industry)"""
    out = _run_westock(["profile", code])
    if not out:
        return "", "", ""
    lines = [l for l in out.split("\n") if l.strip().startswith("|")]
    if len(lines) < 3:
        return "", "", ""
    header = [c.strip() for c in lines[0].strip().strip("|").split("|")]
    data = [c.strip() for c in lines[2].strip().strip("|").split("|")]
    d = dict(zip(header, data))
    return d.get("name", ""), d.get("business", ""), d.get("industry", "")


def get_top_concept(code):
    """westock industry-chain stock -> (绑定度最高概念节点, 关联度, 所属主题)"""
    out = _run_westock(["industry-chain", "stock", code])
    if not out:
        return "", "", ""
    lines = out.split("\n")
    nodes = []
    themes = []
    i = 0
    while i < len(lines):
        if "所属主题" in lines[i]:
            j = i + 1
            while j < len(lines) and not lines[j].strip().startswith("|"):
                j += 1
            if j < len(lines):
                header = [c.strip() for c in lines[j].strip().strip("|").split("|")]
                j += 2
                while j < len(lines) and lines[j].strip().startswith("|"):
                    cells = [c.strip() for c in lines[j].strip().strip("|").split("|")]
                    themes.append(dict(zip(header, cells)))
                    j += 1
        elif "所属节点" in lines[i]:
            j = i + 1
            while j < len(lines) and not lines[j].strip().startswith("|"):
                j += 1
            if j < len(lines):
                header = [c.strip() for c in lines[j].strip().strip("|").split("|")]
                j += 2
                while j < len(lines) and lines[j].strip().startswith("|"):
                    cells = [c.strip() for c in lines[j].strip().strip("|").split("|")]
                    nodes.append(dict(zip(header, cells)))
                    j += 1
        i += 1

    if not nodes:
        return "", "", ""

    def degree(n):
        try:
            return float(n.get("关联度", 0) or 0)
        except Exception:
            return 0.0

    best = max(nodes, key=degree)
    top_deg = degree(best)
    top_nodes = [n for n in nodes if degree(n) == top_deg]
    node_names = "、".join(n.get("节点名称", "") for n in top_nodes[:2])
    theme_names = "、".join(t.get("主题名称", "") for t in themes[:3])
    return node_names, str(int(top_deg)), theme_names


# ============================================================
# 核心：巨量阳K·低位信号检测
# ============================================================
def detect_signal(close, open_, volume, amount, dates):
    """
    在近 SIGNAL_DAYS 个交易日内，找最近的「发生日」同时满足：
      1. 巨量阳K实体长：vol > 前20日均量×2 且 close > open×1.04
      2. 历史极量：vol > 近300日95分位
      3. 低位：close < 近300日55分位
    返回 dict 或 None。
    """
    n = len(close)
    if n < MIN_HISTORY:
        return None

    close = np.asarray(close, dtype=float)
    open_ = np.asarray(open_, dtype=float)
    volume = np.asarray(volume, dtype=float)
    amount = np.asarray(amount, dtype=float)

    scan_start = max(n - SIGNAL_DAYS, PCT_WINDOW)  # 扫描起点，同时保证 300 日窗口完整

    for i in range(n - 1, scan_start - 1, -1):
        # ---- 条件1：巨量 + 实体长 ----
        avg20 = volume[i - VOL_MA:i].mean()
        if avg20 <= 0 or np.isnan(avg20):
            continue
        is_big_vol = volume[i] > avg20 * DOUBLE_VOL
        is_long_body = close[i] > open_[i] * BODY_RATIO
        if not (is_big_vol and is_long_body):
            continue

        # ---- 条件2：历史极量（近300日95分位）----
        win_vol = volume[i - PCT_WINDOW + 1:i + 1]
        win_vol = win_vol[~np.isnan(win_vol)]
        if len(win_vol) < PCT_WINDOW * 0.8:
            continue
        vol_p95 = np.percentile(win_vol, VOL_PCT)
        if volume[i] <= vol_p95:
            continue

        # ---- 条件3：低位（近300日55分位）----
        win_close = close[i - PCT_WINDOW + 1:i + 1]
        win_close = win_close[~np.isnan(win_close)]
        close_p15 = np.percentile(win_close, 15)
        close_p35 = np.percentile(win_close, 35)
        close_p55 = np.percentile(win_close, CLOSE_PCT)
        if close[i] >= close_p55:
            continue

        # ---- 命中 ----
        if close[i] < close_p15:
            low_tier = "极深(<15%)"
        elif close[i] < close_p35:
            low_tier = "深度(15-35%)"
        else:
            low_tier = "低位(35-55%)"

        return {
            "hit_idx": i,
            "hit_date": str(int(dates[i])),
            "hit_close": round(close[i], 2),
            "hit_open": round(open_[i], 2),
            "hit_vol": volume[i],
            "hit_amount": amount[i],       # 千元
            "avg20": avg20,
            "vol_p95": vol_p95,
            "vol_ratio": round(volume[i] / vol_p95, 2),   # 量能 / 95分位 比值
            "close_p15": round(close_p15, 2),
            "close_p35": round(close_p35, 2),
            "close_p55": round(close_p55, 2),
            "low_tier": low_tier,
            "pct_change": round((close[i] - close[i - 1]) / close[i - 1] * 100, 2),
            "body_pct": round((close[i] - open_[i]) / open_[i] * 100, 2),
        }
    return None


# ============================================================
# 走势图
# ============================================================
def plot_price_chart(code, name, open_, high, low, close, volume, dates, hit_idx, out_path):
    """近一年价格 K 线（主图）+ 交易金额副图。金额 = 量(手)×100股 × (开盘+收盘)/2。"""
    n = len(close)
    start = max(0, n - CHART_DAYS)
    xs = list(range(n - start))

    # 成交金额（元）：量(手) × 100(股/手) × (开盘价+收盘价)/2
    amount = volume * 100.0 * (open_ + close) / 2.0
    amount_yi = amount / 1e8  # 亿元

    fig, (ax1, ax2) = plt.subplots(
        2, 1, figsize=(11, 5.4), dpi=110, sharex=True,
        gridspec_kw={"height_ratios": [3, 1]},
    )

    # ---- 主图：价格 K 线（A股涨红跌绿） ----
    for i in range(start, n):
        up = close[i] >= open_[i]
        color = "#d9534f" if up else "#2fa84f"
        if i == hit_idx:
            color = "#ff6a00"  # 发生日橙色高亮
        x = xs[i - start]
        ax1.vlines(x, low[i], high[i], color=color, linewidth=0.8)  # 上下影线
        body_lo = min(open_[i], close[i])
        body_hi = max(open_[i], close[i])
        ax1.bar(x, body_hi - body_lo, bottom=body_lo, color=color, width=0.62, edgecolor=color, linewidth=0.5)

    ax1.set_title(f"{name}（{code}）近一年价格走势", fontsize=11)
    ax1.set_ylabel("价格(元)")
    ax1.grid(axis="y", alpha=0.25)
    ax1.tick_params(labelsize=8)

    # ---- 副图：交易金额 ----
    colors = ["#d9534f" if close[i] >= open_[i] else "#2fa84f" for i in range(start, n)]
    if hit_idx >= start:
        colors[hit_idx - start] = "#ff6a00"
    ax2.bar(xs, amount_yi[start:n], color=colors, width=0.75)
    ax2.set_ylabel("成交额(亿)")
    ax2.set_xlabel("交易日")
    ax2.grid(axis="y", alpha=0.25)
    ax2.tick_params(labelsize=8)
    if hit_idx >= start:
        ax2.annotate(
            f"发生日 {dates[hit_idx]}\n成交额 {amount_yi[hit_idx]:.2f}亿",
            xy=(hit_idx - start, amount_yi[hit_idx]),
            xytext=(hit_idx - start, amount_yi[hit_idx] * 1.08),
            fontsize=8, color="#b04300", ha="center",
        )

    fig.tight_layout()
    fig.savefig(out_path, bbox_inches="tight")
    plt.close(fig)
    return out_path


# ============================================================
# 格式化
# ============================================================
def fmt_amount_yi(amount_1000):
    """千元 -> 亿元"""
    yi = amount_1000 * 1000 / 1e8
    if yi >= 1:
        return f"{yi:.2f}亿"
    return f"{amount_1000 * 1000 / 1e4:.0f}万"


# ============================================================
# 全市场扫描
# ============================================================
def _enrich_one(r, market_df, no_enrich):
    """补字段 + 画走势图（单只，供并发调用）。返回 dict。"""
    ts_code = r["ts_code"]
    wcode = ts_to_westock(ts_code)
    r["wcode"] = wcode
    if not no_enrich:
        name, business, industry = get_profile(wcode)
        concept, degree, themes = get_top_concept(wcode)
        r["name"] = name or r.get("name", "")
        r["business"] = business
        r["industry"] = industry or r.get("industry", "")
        r["concept"] = concept
        r["concept_degree"] = degree
        r["concept_themes"] = themes
    # 走势图（价格K线 + 成交额副图）
    stock_df = market_df[market_df["ts_code"] == ts_code].sort_values("trade_date").reset_index(drop=True)
    close = stock_df["close"].values.astype(float)
    open_ = stock_df["open"].values.astype(float)
    high = stock_df["high"].values.astype(float)
    low = stock_df["low"].values.astype(float)
    vol = stock_df["vol"].values.astype(float)
    dates = stock_df["trade_date"].values
    chart_path = os.path.join(OUT_DIR, f"{ts_code.replace('.', '')}_{r['hit_date']}.png")
    try:
        plot_price_chart(ts_code, r["name"], open_, high, low, close, vol, dates, r["hit_idx"], chart_path)
        r["chart"] = chart_path
    except Exception as e:
        print(f"  [WARN] 画图失败 {ts_code}: {e}", file=sys.stderr, flush=True)
    return r


def run_scan(limit=None, refresh=False, no_enrich=False, max_enrich=None, top=None, workers=8, end_date=None):
    market_df = None
    if not refresh:
        market_df = load_cached_market_daily(end_date)
    if market_df is None:
        print("正在从 tushare 拉取全市场日线（约 60~90 秒）...", flush=True)
        market_df = fetch_full_market_daily(end_date=end_date)
    if market_df is None:
        print("❌ 无法获取数据")
        return []

    monitor_date = parse_date_arg(end_date)
    latest_date = market_df["trade_date"].max()
    print(f"\n巨量阳K·低位监控 | 监控日 {monitor_date.strftime('%Y-%m-%d')} | 数据截至 {latest_date}", flush=True)

    # 股票信息（名称）
    try:
        pro = get_tushare_pro()
        stock_info = pro.stock_basic(exchange="", list_status="L", fields="ts_code,name,industry")
        info_map = {r["ts_code"]: {"name": r["name"], "industry": r["industry"]} for _, r in stock_info.iterrows()}
    except Exception:
        info_map = {}

    ts_codes = market_df["ts_code"].unique()
    if limit:
        ts_codes = ts_codes[:limit]

    total = len(ts_codes)
    print(f"扫描 {total} 只股票...\n", flush=True)

    hits = []
    for i, ts_code in enumerate(ts_codes):
        if (i + 1) % 500 == 0:
            print(f"  进度: {i + 1}/{total}", flush=True)
        stock_df = market_df[market_df["ts_code"] == ts_code].sort_values("trade_date").reset_index(drop=True)
        if len(stock_df) < MIN_HISTORY:
            continue
        close = stock_df["close"].values.astype(float)
        open_ = stock_df["open"].values.astype(float)
        vol = stock_df["vol"].values.astype(float)
        amount = stock_df["amount"].values.astype(float)
        dates = stock_df["trade_date"].values

        if np.any(close <= 0) or np.any(vol < 0):
            continue

        r = detect_signal(close, open_, vol, amount, dates)
        if r is None:
            continue
        info = info_map.get(ts_code, {})
        r["ts_code"] = ts_code
        r["name"] = info.get("name", "")
        r["industry"] = info.get("industry", "")
        hits.append(r)
        print(f"  >>> HIT {ts_code} {r['name']} | {r['hit_date']} | 量能/95分位 {r['vol_ratio']}x | {r['low_tier']}", flush=True)

    # 按量能比值降序
    hits.sort(key=lambda x: -x["vol_ratio"])
    print(f"\n命中 {len(hits)}/{total} 只", flush=True)

    if not hits:
        print("今日无「巨量阳K·低位」信号")
        return []

    # --top N：只保留量能比值最高的 N 只（截断发生在补字段/画图之前，省时）
    if top and top < len(hits):
        print(f"仅保留量能比值最高的 {top} 只（其余 {len(hits) - top} 只略过）", flush=True)
        hits = hits[:top]

    # 并发补字段 + 走势图
    os.makedirs(OUT_DIR, exist_ok=True)
    done = 0
    if workers > 1 and len(hits) > 1:
        with ThreadPoolExecutor(max_workers=workers) as ex:
            futs = {ex.submit(_enrich_one, r, market_df, no_enrich): r for r in hits}
            for fut in as_completed(futs):
                done += 1
                if done % 20 == 0 or done == len(hits):
                    print(f"  补全进度: {done}/{len(hits)}", flush=True)
                try:
                    futs[fut] = fut.result()
                except Exception as e:
                    print(f"  [WARN] {futs[fut]['ts_code']}: {e}", file=sys.stderr, flush=True)
        hits = [futs[fut] for fut in futs]
        # 保持原排序（as_completed 无序）
        hits.sort(key=lambda x: -x["vol_ratio"])
    else:
        for r in hits:
            _enrich_one(r, market_df, no_enrich)
            done += 1
            if done % 20 == 0:
                print(f"  补全进度: {done}/{len(hits)}", flush=True)

    return hits


def print_report(hits):
    print("\n" + "=" * 100)
    print("「巨量阳K·低位」命中结果")
    print("=" * 100)
    for idx, r in enumerate(hits):
        print(f"\n[{idx + 1}] {r['ts_code']} {r['name']}（{r.get('industry', '')}）")
        print(f"    发生日: {r['hit_date']} | 收 {r['hit_close']}({r['pct_change']:+.2f}%) | 实体 {r['body_pct']:+.2f}%")
        print(f"    主营业务: {r.get('business', '')[:60]}")
        print(f"    绑定度最高概念: {r.get('concept', '')}（关联度 {r.get('concept_degree', '')}）")
        print(f"    发生日交易额: {fmt_amount_yi(r['hit_amount'])}")
        print(f"    量能/300日95分位: {r['vol_ratio']}x")
        print(f"    低位档: {r['low_tier']}（300日收盘价 15/35/55分位 = {r['close_p15']}/{r['close_p35']}/{r['close_p55']}）")
        if r.get("chart"):
            print(f"    走势图: {r['chart']}")
    print("\n" + "=" * 100)


def write_csv(hits):
    """输出 8 列表格为 CSV（走势图列存图片文件名）。"""
    os.makedirs(OUT_DIR, exist_ok=True)
    rows = []
    for r in hits:
        rows.append({
            "A股代码": r["ts_code"],
            "A股名称": r.get("name", ""),
            "主营业务": (r.get("business", "") or "")[:80],
            "绑定度最高概念": f"{r.get('concept', '')}({r.get('concept_degree', '')})",
            "发生日交易额": fmt_amount_yi(r["hit_amount"]),
            "量能/95分位比值": r["vol_ratio"],
            "价格走势+成交额图": os.path.basename(r.get("chart", "")) or "",
            "低位档": r["low_tier"],
            "发生日": r["hit_date"],
            "发生日涨跌幅%": r["pct_change"],
        })
    df = pd.DataFrame(rows)
    csv_path = os.path.join(OUT_DIR, "巨量阳K低位_命中列表.csv")
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    print(f"\nCSV 已保存: {csv_path}")
    return csv_path


def write_html(hits, monitor_date=None):
    """生成可浏览的 HTML 报告（表格 + 内嵌走势图）。"""
    os.makedirs(OUT_DIR, exist_ok=True)
    html_path = os.path.join(OUT_DIR, "巨量阳K低位_监控报告.html")
    md = parse_date_arg(monitor_date).strftime("%Y-%m-%d")

    rows_html = []
    for i, r in enumerate(hits):
        chart = r.get("chart", "")
        chart_tag = ""
        if chart and os.path.exists(chart):
            chart_tag = f'<img src="{os.path.basename(chart)}" style="width:260px;border:1px solid #444;">'
        tier_color = {"极深(<15%)": "#e74c3c", "深度(15-35%)": "#e67e22", "低位(35-55%)": "#f1c40f"}.get(r["low_tier"], "#999")
        concept = r.get("concept", "") or "-"
        degree = r.get("concept_degree", "") or ""
        rows_html.append(
            f'<tr>'
            f'<td>{i + 1}</td>'
            f'<td>{r["ts_code"]}</td>'
            f'<td>{r.get("name", "")}</td>'
            f'<td class="biz">{(r.get("business", "") or "")[:60]}</td>'
            f'<td>{concept}<br><span class="muted">关联度 {degree}</span></td>'
            f'<td>{r["hit_date"]}</td>'
            f'<td>{fmt_amount_yi(r["hit_amount"])}</td>'
            f'<td class="vol">{r["vol_ratio"]:.2f}x</td>'
            f'<td><span class="tier" style="background:{tier_color}">{r["low_tier"]}</span></td>'
            f'<td>{chart_tag}</td>'
            f'</tr>'
        )

    html = f"""<!DOCTYPE html>
<html lang="zh"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>巨量阳K·低位监控报告</title>
<style>
body{{background:#111;color:#eee;font-family:-apple-system,"PingFang SC","Microsoft YaHei",sans-serif;margin:0;padding:24px;}}
h1{{font-size:20px;}} h2{{font-size:15px;color:#aaa;font-weight:normal;}}
table{{border-collapse:collapse;width:100%;font-size:12px;margin-top:16px;}}
th,td{{border:1px solid #333;padding:6px 8px;text-align:left;vertical-align:top;}}
th{{background:#1e2a3a;position:sticky;top:0;}}
tr:nth-child(even){{background:#181818;}}
.vol{{color:#ff6a00;font-weight:bold;white-space:nowrap;}}
.tier{{color:#fff;padding:2px 8px;border-radius:10px;font-size:11px;white-space:nowrap;}}
.biz{{color:#bbb;max-width:220px;}}
.muted{{color:#777;font-size:10px;}}
.summary{{color:#ccc;line-height:1.8;}}
</style></head><body>
<h1>巨量阳K·低位监控报告</h1>
<h2>三条件 AND：①量>前20日均量×2 且 收盘>开盘×1.04 ②量>300日95分位 ③收盘<300日55分位（近30日取最近发生日）</h2>
<p class="summary">监控日 <b style="color:#ff6a00">{md}</b> | 命中 <b style="color:#ff6a00">{len(hits)}</b> 只 | 生成时间 {datetime.now().strftime('%Y-%m-%d %H:%M')} | 按「量能/95分位比值」降序</p>
<table>
<thead><tr>
<th>#</th><th>代码</th><th>名称</th><th>主营业务</th><th>绑定度最高概念</th><th>发生日</th><th>交易额</th><th>量能/95分位</th><th>低位档</th><th>近一年价格走势+成交额</th>
</tr></thead>
<tbody>
{''.join(rows_html)}
</tbody></table>
</body></html>"""
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"HTML 报告已保存: {html_path}")
    return html_path


def check_single(wcode, end_date=None):
    """单股诊断（westock 日线，前复权）。end_date 可指定历史监控日。"""
    wcode = wcode.lower()
    if not (wcode.startswith("sh") or wcode.startswith("sz")):
        print("代码需带市场前缀，如 sz300866")
        return None
    end_dt = parse_date_arg(end_date)
    if end_date is None:
        # 默认：拉最近 SCAN_LOOKBACK 个交易日
        out = _run_westock(["kline", wcode, "--period", "day", "--limit", str(SCAN_LOOKBACK), "--fq", "qfq"], timeout=60)
    else:
        # 自定义监控日：按日期范围拉（start 往前多取，确保 300 日窗口完整）
        start_dt = end_dt - timedelta(days=SCAN_LOOKBACK * 2)
        out = _run_westock(
            ["kline", wcode, "--period", "day",
             "--start", fmt_date_arg(start_dt), "--end", fmt_date_arg(end_dt),
             "--limit", str(SCAN_LOOKBACK + 30), "--fq", "qfq"],
            timeout=60)
    if not out:
        print(f"无法获取 {wcode} K线")
        return None
    lines = [l for l in out.split("\n") if l.strip().startswith("|")]
    if len(lines) < 3:
        print(f"{wcode} K线数据不足")
        return None
    header = [c.strip() for c in lines[0].strip().strip("|").split("|")]
    rows = []
    for l in lines[2:]:
        cells = [c.strip() for c in l.strip().strip("|").split("|")]
        rows.append(dict(zip(header, cells)))
    df = pd.DataFrame(rows)
    df = df.iloc[::-1].reset_index(drop=True)  # westock 返回最新在前，翻转为时间正序

    close = df["last"].astype(float).values
    open_ = df["open"].astype(float).values
    high = df["high"].astype(float).values
    low = df["low"].astype(float).values
    vol = df["volume"].astype(float).values
    amount = df["amount"].astype(float).values
    dates = df["date"].values

    r = detect_signal(close, open_, vol, amount, dates)
    if r is None:
        print(f"{wcode} 未检测到「巨量阳K·低位」信号（近{SIGNAL_DAYS}日内）")
        return None
    name, business, industry = get_profile(wcode)
    concept, degree, themes = get_top_concept(wcode)
    r["ts_code"] = wcode
    r["name"] = name
    r["industry"] = industry
    r["business"] = business
    r["concept"] = concept
    r["concept_degree"] = degree
    r["concept_themes"] = themes
    r["hit_amount"] = amount[r["hit_idx"]] / 1000.0  # westock amount 单位是元，转千元
    os.makedirs(OUT_DIR, exist_ok=True)
    chart_path = os.path.join(OUT_DIR, f"{wcode}_{r['hit_date']}.png")
    plot_price_chart(wcode, "", open_, high, low, close, vol, dates, r["hit_idx"], chart_path)
    r["chart"] = chart_path
    print_report([r])
    return r


# ============================================================
# 命令行入口
# ============================================================
def main():
    parser = argparse.ArgumentParser(description="全A股巨量阳K·低位监控 v4.0")
    sub = parser.add_subparsers(dest="command")

    scan_p = sub.add_parser("scan", help="全市场扫描")
    scan_p.add_argument("--limit", type=int, default=None)
    scan_p.add_argument("--refresh", action="store_true")
    scan_p.add_argument("--date", dest="end_date", default=None,
                        help="监控日（默认=系统今天），格式 YYYYMMDD 或 YYYY-MM-DD，如 20240924")
    scan_p.add_argument("--no-enrich", action="store_true", help="不调 westock 补主营业务/概念")
    scan_p.add_argument("--max-enrich", type=int, default=None, help="最多补 N 只字段（已弃用，保留兼容）")
    scan_p.add_argument("--top", type=int, default=None, help="仅保留量能比值最高的 N 只（截断后再补字段/画图）")
    scan_p.add_argument("--workers", type=int, default=8, help="并发补字段线程数")
    scan_p.add_argument("--no-html", action="store_true", help="不生成 HTML 报告")

    check_p = sub.add_parser("check", help="单股诊断（westock 日线）")
    check_p.add_argument("code", help="如 sz300866")
    check_p.add_argument("--date", dest="end_date", default=None,
                         help="监控日（默认=系统今天），格式 YYYYMMDD 或 YYYY-MM-DD，如 20240924")

    args = parser.parse_args()
    if args.command == "scan":
        hits = run_scan(limit=args.limit, refresh=args.refresh,
                        no_enrich=args.no_enrich, max_enrich=args.max_enrich,
                        top=args.top, workers=args.workers, end_date=args.end_date)
        if hits:
            print_report(hits)
            write_csv(hits)
            if not args.no_html:
                write_html(hits, monitor_date=args.end_date)
    elif args.command == "check":
        check_single(args.code, end_date=args.end_date)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
