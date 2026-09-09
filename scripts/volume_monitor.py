#!/usr/bin/env python3
"""
量能异动监控 v3.0

核心逻辑：量能异动是前置雷达信号，不是后视镜确认。

正确逻辑链：
  低位/相对低位量能异动 → 关注 → 等金叉确认 → 等回调到均线 → 买入

v3.0 改动：
  - scan 命令使用 tushare 批量拉取全市场日线数据（5200+只）
  - 真正覆盖沪深主板 + 创业板 + 科创板
  - check/watchlist 保留 westock-data（支持港股/美股）

用法:
  python volume_monitor.py scan                    # 全市场低位量能异动扫描
  python volume_monitor.py scan --limit 200        # 限制扫描数量(调试用)
  python volume_monitor.py check sz002368          # 单股低位异动诊断
  python volume_monitor.py watchlist               # 批量检查监控列表
  python volume_monitor.py add sz002368 太极股份    # 添加监控
  python volume_monitor.py remove sz002368         # 移除监控
  python volume_monitor.py list                    # 查看列表
  python volume_monitor.py refresh                 # 刷新tushare全市场数据缓存
"""

import numpy as np
import pandas as pd
import talib
import subprocess
import json
import os
import sys
import argparse
import time
from datetime import datetime, timedelta


# ============================================================
# 配置
# ============================================================
SKILL_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
WATCHLIST_PATH = os.path.join(SKILL_DIR, "watchlist.json")
SIDEWAY_WATCHLIST_PATH = os.path.join(SKILL_DIR, "sideway_watchlist.json")
CACHE_DIR = os.path.join(SKILL_DIR, "cache")
WESTOCK_DIR = os.path.expanduser(
    "~/.workbuddy/plugins/marketplaces/cb_teams_marketplace/plugins/finance-data/skills/westock-data"
)

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


SCAN_LOOKBACK = 130  # 扫描用回溯天数
SCAN_MIN_HISTORY = 80  # 最低历史数据要求

# 横盘缩量监控参数
SIDEWAY_DAYS = 3          # 连续横盘天数
SIDEWAY_AMPLITUDE = 3.0   # 横盘振幅阈值(%): N日内最高-最低 / 最低 < 此值
SIDEWAY_VOL_RATIO = 0.40  # 极致缩量: 当日量 < 20日均量 * 此比例


# ============================================================
# 数据获取: tushare (全市场批量)
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


def fetch_full_market_daily(lookback=SCAN_LOOKBACK):
    """
    用tushare批量拉取全市场日线数据。
    按日期循环，每次返回全市场当日数据，拼接成大表。
    返回: DataFrame (ts_code, trade_date, open, high, low, close, pre_close, pct_chg, vol, amount)
    """
    pro = get_tushare_pro()
    
    # 先获取交易日历
    end_date = datetime.now().strftime("%Y%m%d")
    start_date = (datetime.now() - timedelta(days=lookback * 2)).strftime("%Y%m%d")  # 多拉一些确保够
    
    cal = pro.trade_cal(exchange='SSE', start_date=start_date, end_date=end_date, is_open='1')
    trade_dates = sorted(cal['cal_date'].tolist(), reverse=True)[:lookback]
    
    print(f"拉取全市场日线 | {trade_dates[-1]}~{trade_dates[0]} | {len(trade_dates)}个交易日")
    
    all_dfs = []
    for i, date in enumerate(trade_dates):
        try:
            df = pro.daily(trade_date=date)
            if df is not None and len(df) > 0:
                all_dfs.append(df)
                if (i + 1) % 10 == 0:
                    print(f"  进度: {i + 1}/{len(trade_dates)} | {date} | {len(df)}条")
            time.sleep(0.15)  # tushare限速
        except Exception as e:
            print(f"  [WARN] {date}: {e}", file=sys.stderr)
            time.sleep(0.5)
    
    if not all_dfs:
        print("❌ 未能获取任何数据")
        return None
    
    combined = pd.concat(all_dfs, ignore_index=True)
    
    # 过滤沪深A股
    combined = combined[combined['ts_code'].str.match(r'^(60|68|00|30)\d{4}\.(SH|SZ)$')]
    
    # 按股票代码+日期排序
    combined = combined.sort_values(['ts_code', 'trade_date']).reset_index(drop=True)
    
    print(f"全市场数据: {len(combined)}条 | {combined['ts_code'].nunique()}只股票")
    
    # 保存缓存
    os.makedirs(CACHE_DIR, exist_ok=True)
    cache_path = os.path.join(CACHE_DIR, f"market_daily_{end_date}.parquet")
    combined.to_parquet(cache_path, index=False)
    print(f"缓存已保存: {cache_path}")
    
    return combined


def load_cached_market_daily():
    """加载缓存的最新全市场数据"""
    os.makedirs(CACHE_DIR, exist_ok=True)
    cache_files = sorted([f for f in os.listdir(CACHE_DIR) if f.startswith("market_daily_") and f.endswith(".parquet")])
    if not cache_files:
        return None
    latest = os.path.join(CACHE_DIR, cache_files[-1])
    df = pd.read_parquet(latest)
    # 检查缓存是否今天
    cache_date = cache_files[-1].replace("market_daily_", "").replace(".parquet", "")
    today = datetime.now().strftime("%Y%m%d")
    if cache_date < today:
        print(f"⚠️ 缓存日期 {cache_date} 早于今日 {today}，建议刷新")
    return df


# ============================================================
# 数据获取: westock-data (单股查询，支持港股/美股)
# ============================================================
def fetch_kline(code, period="day", limit=250):
    """通过westock-data获取K线数据（用于check/watchlist）"""
    try:
        result = subprocess.run(
            ["node", "scripts/index.js", "kline", code, "--period", period, "--limit", str(limit)],
            capture_output=True, text=True, cwd=WESTOCK_DIR, timeout=30
        )
        if result.returncode != 0:
            return None

        lines = result.stdout.strip().split('\n')
        if len(lines) < 3:
            return None

        header_cells = [c.strip() for c in lines[0].split('|')]
        col_index = {}
        for i, h in enumerate(header_cells):
            h_lower = h.lower().strip()
            if h_lower == 'date': col_index['date'] = i
            elif h_lower == 'open': col_index['open'] = i
            elif h_lower == 'last': col_index['close'] = i
            elif h_lower == 'high': col_index['high'] = i
            elif h_lower == 'low': col_index['low'] = i
            elif h_lower == 'volume': col_index['volume'] = i
            elif h_lower == 'amount': col_index['amount'] = i
            elif h_lower in ('exchange', 'turnover', '换手率'): col_index['turnover'] = i

        required = ['close', 'high', 'low', 'volume']
        if not all(k in col_index for k in required):
            return None

        data = {k: [] for k in col_index}
        for line in lines[2:]:
            cells = [c.strip() for c in line.split('|')]
            for key, idx in col_index.items():
                v = cells[idx] if idx < len(cells) else '0'
                v_clean = v.replace('%', '').replace(',', '').strip()
                if key == 'date':
                    v_clean = v_clean.replace('-', '')
                    try:
                        data[key].append(int(v_clean) if v_clean and v_clean != '-' else 0)
                    except ValueError:
                        data[key].append(0)
                else:
                    try:
                        data[key].append(float(v_clean) if v_clean and v_clean != '-' else 0)
                    except ValueError:
                        data[key].append(0)

        df = pd.DataFrame(data)
        if len(df) < 60:
            return None

        if 'date' in df.columns:
            df = df.iloc[::-1].reset_index(drop=True)

        return df
    except Exception as e:
        print(f"  [ERR] fetch_kline {code}: {e}", file=sys.stderr)
        return None


# ============================================================
# 知行均线计算
# ============================================================
def calc_zhixing_fast(close):
    """双重平滑10日EMA"""
    ema1 = talib.EMA(close, timeperiod=10)
    return talib.EMA(ema1, timeperiod=10)


def calc_zhixing_slow(close):
    """四条MA的均值: MA14 + MA28 + MA57 + MA114"""
    ma14 = talib.MA(close, timeperiod=14)
    ma28 = talib.MA(close, timeperiod=28)
    ma57 = talib.MA(close, timeperiod=57)
    ma114 = talib.MA(close, timeperiod=114)
    return (ma14 + ma28 + ma57 + ma114) / 4


def calc_kdj(high, low, close):
    """计算KDJ"""
    k, d = talib.STOCH(high, low, close,
                       fastk_period=9, slowk_period=3, slowk_matype=0,
                       slowd_period=3, slowd_matype=0)
    j = 3 * k - 2 * d
    return k, d, j


# ============================================================
# 核心：低位量能异动检测
# ============================================================
def check_low_volume_anomaly_arrays(close, high, low, volume, date_str=""):
    """
    低位量能异动检测（纯数组版本，tushare和westock通用）。
    
    逻辑：量能异动 + 低位 = 前置关注信号
    低位判断：
      A. 股价在近120日百分位 < 40%
      B. 股价在MA60下方或刚触及（距离 < +5%）
    
    量能异动：
      1. 倍量柱：当日量 >= 前一日2倍
      2. 历史极量：当日量在近120日百分位 >= 95%
      3. 温和放量：当日量 >= 20日均量1.5倍
    
    排除：
      - 涨停板+倍量柱+局部高点倍量 → 出货
    
    返回: dict 或 None
    """
    end_idx = len(close) - 1
    if end_idx < 60:
        return None

    cur_close = close[end_idx]
    cur_vol = volume[end_idx]
    prev_vol = volume[end_idx - 1] if end_idx >= 1 else 0

    # ============ 低位判断 ============
    
    # A. 近120日价格百分位
    lookback_price = min(120, end_idx + 1)
    price_window = close[max(0, end_idx - lookback_price + 1):end_idx + 1]
    price_pct = np.sum(price_window <= cur_close) / len(price_window) * 100

    # B. MA60 距离
    ma60 = talib.MA(close, timeperiod=60)
    ma60 = np.nan_to_num(ma60, nan=0)
    cur_ma60 = ma60[end_idx]
    dist_ma60 = (cur_close - cur_ma60) / cur_ma60 * 100 if cur_ma60 > 0 else 999

    # C. 知行均线状态
    zf = calc_zhixing_fast(close)
    zs = calc_zhixing_slow(close)
    zf = np.nan_to_num(zf, nan=0)
    zs = np.nan_to_num(zs, nan=0)
    cur_zf = zf[end_idx]
    cur_zs = zs[end_idx]
    
    is_bullish_align = cur_zf > cur_zs
    dist_fast_slow = (cur_zf - cur_zs) / cur_zs * 100 if cur_zs > 0 else 0
    is_near_cross = -5 <= dist_fast_slow <= 3

    # 低位综合判断
    is_low_position = (price_pct < 40 or dist_ma60 < 5)
    
    if not is_low_position and not is_near_cross:
        return None

    # ============ 量能异动判断 ============
    
    avg_vol_20 = np.mean(volume[max(0, end_idx - 20):end_idx + 1])

    # 1. 倍量柱
    is_double_vol = False
    vol_ratio_prev = 0
    if prev_vol > 0 and not np.isnan(prev_vol):
        vol_ratio_prev = cur_vol / prev_vol
        is_double_vol = cur_vol >= prev_vol * 2

    # 2. 历史极量
    lookback_vol = min(120, end_idx + 1)
    vol_window = volume[max(0, end_idx - lookback_vol + 1):end_idx + 1]
    vol_valid = vol_window[~np.isnan(vol_window)]
    vol_rank_pct = np.sum(vol_valid <= cur_vol) / len(vol_valid) * 100 if len(vol_valid) > 0 else 0
    is_top5_volume = vol_rank_pct >= 95

    # 3. 温和放量
    vol_ratio_20avg = cur_vol / avg_vol_20 if avg_vol_20 > 0 else 0
    is_above_1_5x_avg = vol_ratio_20avg >= 1.5

    # ============ 排除条件 ============
    
    pct_change = 0
    if end_idx >= 1 and close[end_idx - 1] > 0:
        pct_change = (cur_close - close[end_idx - 1]) / close[end_idx - 1] * 100
    is_limit_up = pct_change > 9.5

    # 局部高点±1天倍量柱
    lookback_top = min(20, end_idx + 1)
    recent_highs = close[max(0, end_idx - lookback_top + 1):end_idx + 1]
    local_high_offset = np.argmax(recent_highs)
    local_high_idx = end_idx - lookback_top + 1 + local_high_offset
    top_has_double_vol = False
    for i in range(max(0, local_high_idx - 1), min(local_high_idx + 2, end_idx + 1)):
        if i < 1:
            continue
        if volume[i] >= volume[i - 1] * 2 and volume[i - 1] > 0:
            top_has_double_vol = True
            break

    # 涨停板+倍量柱+顶部 → 出货
    if is_limit_up and is_double_vol and top_has_double_vol:
        return None

    # 量能无异动
    has_volume_anomaly = is_double_vol or is_top5_volume or is_above_1_5x_avg
    if not has_volume_anomaly:
        return None

    # ============ 综合评级 ============
    
    if is_double_vol and is_top5_volume:
        vol_level = "⚡⚡⚡"
        vol_desc = f"倍量柱({vol_ratio_prev:.1f}x) + 极量(top{100-vol_rank_pct:.0f}%)"
    elif is_double_vol:
        vol_level = "⚡⚡"
        vol_desc = f"倍量柱({vol_ratio_prev:.1f}x前日)"
    elif is_top5_volume:
        vol_level = "⚡⚡"
        vol_desc = f"历史极量(百分位{vol_rank_pct:.0f}%)"
    elif is_above_1_5x_avg:
        vol_level = "⚡"
        vol_desc = f"温和放量({vol_ratio_20avg:.1f}x均量)"
    else:
        return None

    if price_pct < 20:
        low_level = "深度低位"
    elif price_pct < 40:
        low_level = "相对低位"
    elif is_near_cross and not is_bullish_align:
        low_level = "金叉前夕"
    elif is_near_cross and is_bullish_align:
        low_level = "刚金叉"
    else:
        low_level = "MA60附近"

    if is_near_cross and not is_bullish_align:
        follow_path = "→ 等金叉确认"
    elif is_near_cross and is_bullish_align:
        follow_path = "→ 金叉已出，等回调买入"
    elif is_bullish_align and dist_fast_slow > 3:
        follow_path = "→ 多头排列中，等回调到均线"
    else:
        follow_path = "→ 等均线收敛/金叉"

    risk_tags = []
    if top_has_double_vol:
        risk_tags.append("🔺局部高点有倍量柱")
    if is_limit_up:
        risk_tags.append("🔺涨停板放量")
    if pct_change < -3:
        risk_tags.append("⚠️大跌放量")

    k_arr, d_arr, j_arr = calc_kdj(high, low, close)
    j_arr = np.nan_to_num(j_arr, nan=50)
    cur_j = j_arr[end_idx]

    dist_zf = (cur_close - cur_zf) / cur_zf * 100 if cur_zf > 0 else 999
    dist_zs = (cur_close - cur_zs) / cur_zs * 100 if cur_zs > 0 else 999

    return {
        'date': date_str,
        'close': round(cur_close, 2),
        'pct_change': round(pct_change, 2),
        'vol_level': vol_level,
        'vol_desc': vol_desc,
        'low_level': low_level,
        'follow_path': follow_path,
        'price_pct': round(price_pct, 1),
        'dist_ma60': round(dist_ma60, 2),
        'dist_fast_slow': round(dist_fast_slow, 2),
        'is_bullish_align': is_bullish_align,
        'is_near_cross': is_near_cross,
        'cur_vol': int(cur_vol),
        'prev_vol': int(prev_vol),
        'avg_vol_20': int(avg_vol_20),
        'vol_ratio_prev': round(vol_ratio_prev, 2),
        'vol_ratio_20avg': round(vol_ratio_20avg, 2),
        'vol_rank_pct': round(vol_rank_pct, 1),
        'is_double_vol': is_double_vol,
        'is_top5_volume': is_top5_volume,
        'top_has_double_vol': top_has_double_vol,
        'is_limit_up': is_limit_up,
        'kdj_j': round(cur_j, 1),
        'dist_zf': round(dist_zf, 2),
        'dist_zs': round(dist_zs, 2),
        'cur_zf': round(cur_zf, 2),
        'cur_zs': round(cur_zs, 2),
        'cur_ma60': round(cur_ma60, 2),
        'risk_tags': risk_tags,
    }


def check_low_volume_anomaly(df, code, name=""):
    """从DataFrame检测低位量能异动（westock-data格式）"""
    close = np.array(df['close'], dtype=float)
    high = np.array(df['high'], dtype=float)
    low = np.array(df['low'], dtype=float)
    volume = np.array(df['volume'], dtype=float)

    end_idx = len(close) - 1
    cur_date = str(int(df['date'].iloc[end_idx])) if 'date' in df.columns else "N/A"

    r = check_low_volume_anomaly_arrays(close, high, low, volume, cur_date)
    if r is not None:
        r['code'] = code
        r['name'] = name
    return r


# ============================================================
# 横盘缩量监控
# ============================================================
def check_sideway_consolidation(close, high, low, volume, date_str=""):
    """
    连续横盘 + 极致缩量检测。

    条件：
      1. 最近 SIDEWAY_DAYS 天收盘价振幅 < SIDEWAY_AMPLITUDE%
      2. 最近 SIDEWAY_DAYS 天每日成交量均 < 20日均量 * SIDEWAY_VOL_RATIO

    返回: dict 或 None
    """
    end_idx = len(close) - 1
    if end_idx < 20 + SIDEWAY_DAYS:
        return None

    # 最近N天数据
    recent_closes = close[end_idx - SIDEWAY_DAYS + 1:end_idx + 1]
    recent_highs = high[end_idx - SIDEWAY_DAYS + 1:end_idx + 1]
    recent_lows = low[end_idx - SIDEWAY_DAYS + 1:end_idx + 1]
    recent_vols = volume[end_idx - SIDEWAY_DAYS + 1:end_idx + 1]

    # 1. 横盘判断: N日内 (最高价-最低价)/最低价 < SIDEWAY_AMPLITUDE%
    period_high = np.max(recent_highs)
    period_low = np.min(recent_lows)
    if period_low <= 0:
        return None
    amplitude = (period_high - period_low) / period_low * 100

    is_sideway = amplitude < SIDEWAY_AMPLITUDE

    # 2. 极致缩量: 每日量均 < 20日均量 * SIDEWAY_VOL_RATIO
    avg_vol_20 = np.mean(volume[end_idx - 20:end_idx + 1])
    if avg_vol_20 <= 0:
        return None

    vol_ratios = recent_vols / avg_vol_20
    all_shrink = np.all(vol_ratios < SIDEWAY_VOL_RATIO)
    avg_shrink_ratio = np.mean(vol_ratios)

    # 3. 趋势背景: 之前是上涨后回调到横盘
    # 用知行均线判断当前趋势
    zf = calc_zhixing_fast(close)
    zs = calc_zhixing_slow(close)
    zf = np.nan_to_num(zf, nan=0)
    zs = np.nan_to_num(zs, nan=0)
    ma60 = np.nan_to_num(talib.MA(close, timeperiod=60), nan=0)

    cur_close = close[end_idx]
    cur_zf = zf[end_idx]
    cur_zs = zs[end_idx]
    cur_ma60 = ma60[end_idx]

    dist_zf = (cur_close - cur_zf) / cur_zf * 100 if cur_zf > 0 else 999
    dist_zs = (cur_close - cur_zs) / cur_zs * 100 if cur_zs > 0 else 999
    dist_ma60 = (cur_close - cur_ma60) / cur_ma60 * 100 if cur_ma60 > 0 else 999

    is_bullish = cur_zf > cur_zs

    # 120日价格位置
    lookback_price = min(120, end_idx + 1)
    price_window = close[max(0, end_idx - lookback_price + 1):end_idx + 1]
    price_pct = np.sum(price_window <= cur_close) / len(price_window) * 100

    # KDJ
    k, d, j = calc_kdj(high, low, close)
    j = np.nan_to_num(j, nan=50)

    if not is_sideway:
        return None

    # 缩量程度评级
    if avg_shrink_ratio < 0.2:
        shrink_level = "🔴 极致缩量"
        shrink_desc = f"均量仅为20日均量的{avg_shrink_ratio:.0%}"
    elif avg_shrink_ratio < 0.3:
        shrink_level = "🟠 严重缩量"
        shrink_desc = f"均量仅为20日均量的{avg_shrink_ratio:.0%}"
    elif avg_shrink_ratio < 0.4:
        shrink_level = "🟡 明显缩量"
        shrink_desc = f"均量仅为20日均量的{avg_shrink_ratio:.0%}"
    else:
        shrink_level = "⚪ 轻微缩量"
        shrink_desc = f"均量为20日均量的{avg_shrink_ratio:.0%}"

    # 横盘评级
    if amplitude < 1.0:
        sideway_level = "极度横盘"
    elif amplitude < 2.0:
        sideway_level = "明显横盘"
    else:
        sideway_level = "轻微横盘"

    signal = all_shrink and is_sideway

    return {
        'date': date_str,
        'close': round(cur_close, 2),
        'amplitude': round(amplitude, 2),
        'sideway_days': SIDEWAY_DAYS,
        'sideway_level': sideway_level,
        'shrink_level': shrink_level if all_shrink else "未缩量",
        'shrink_desc': shrink_desc if all_shrink else f"均量为20日均量的{avg_shrink_ratio:.0%}",
        'all_shrink': all_shrink,
        'avg_shrink_ratio': round(avg_shrink_ratio, 3),
        'vol_ratios': [round(v, 3) for v in vol_ratios],
        'avg_vol_20': int(avg_vol_20),
        'is_bullish': is_bullish,
        'dist_zf': round(dist_zf, 2),
        'dist_zs': round(dist_zs, 2),
        'dist_ma60': round(dist_ma60, 2),
        'price_pct': round(price_pct, 1),
        'cur_zf': round(cur_zf, 2),
        'cur_zs': round(cur_zs, 2),
        'cur_ma60': round(cur_ma60, 2),
        'kdj_j': round(j[end_idx], 1),
        'signal': signal,  # 完整信号: 横盘+极致缩量
    }


def check_sideway_single(code, name=""):
    """单股横盘缩量诊断"""
    df = fetch_kline(code)
    if df is None:
        print(f"无法获取 {code} 的K线数据")
        return None

    close = np.array(df['close'], dtype=float)
    high = np.array(df['high'], dtype=float)
    low = np.array(df['low'], dtype=float)
    volume = np.array(df['volume'], dtype=float)

    end_idx = len(close) - 1
    cur_date = str(int(df['date'].iloc[end_idx])) if 'date' in df.columns else "N/A"

    r = check_sideway_consolidation(close, high, low, volume, cur_date)
    if r is not None:
        r['code'] = code
        r['name'] = name
        _print_sideway_report(r)
    else:
        print(f"  {code} {name} — 未检测到横盘信号")
    return r


def _print_sideway_report(r):
    """打印横盘缩量报告"""
    code_name = f"{r.get('code','')} {r.get('name','')}".strip()
    signal_tag = "✅ 信号触发" if r['signal'] else "⏳ 监控中"
    print(f"  {code_name} | {r['date']} | 收{r['close']} | {signal_tag}")
    print(f"  横盘: {r['sideway_level']} | {r['sideway_days']}日振幅{r['amplitude']}%")
    print(f"  缩量: {r['shrink_level']} | {r['shrink_desc']}")
    print(f"  逐日量比: {' / '.join(f'{v:.1%}' for v in r['vol_ratios'])}")
    print(f"  均线: fast={r['cur_zf']} slow={r['cur_zs']} MA60={r['cur_ma60']}")
    print(f"  位置: 距fast {r['dist_zf']:+.2f}% | 距slow {r['dist_zs']:+.2f}% | 距MA60 {r['dist_ma60']:+.2f}%")
    print(f"  120日百分位: {r['price_pct']}% | KDJ.J={r['kdj_j']} | {'多头' if r['is_bullish'] else '空头'}排列")


# ============================================================
# 横盘缩量 Watchlist
# ============================================================
def load_sideway_watchlist():
    if not os.path.exists(SIDEWAY_WATCHLIST_PATH):
        return []
    with open(SIDEWAY_WATCHLIST_PATH, 'r', encoding='utf-8') as f:
        return json.load(f)


def save_sideway_watchlist(wl):
    with open(SIDEWAY_WATCHLIST_PATH, 'w', encoding='utf-8') as f:
        json.dump(wl, f, ensure_ascii=False, indent=2)


def add_to_sideway_watchlist(code, name=""):
    wl = load_sideway_watchlist()
    if any(item['code'] == code for item in wl):
        print(f"{code} 已在横盘缩量监控列表中")
        return
    wl.append({"code": code, "name": name, "added": datetime.now().strftime('%Y-%m-%d')})
    save_sideway_watchlist(wl)
    print(f"✅ 已添加 {code} {name} 到横盘缩量监控列表")


def remove_from_sideway_watchlist(code):
    wl = load_sideway_watchlist()
    original_len = len(wl)
    wl = [item for item in wl if item['code'] != code]
    if len(wl) == original_len:
        print(f"{code} 不在横盘缩量监控列表中")
        return
    save_sideway_watchlist(wl)
    print(f"✅ 已从横盘缩量监控列表移除 {code}")


def list_sideway_watchlist():
    wl = load_sideway_watchlist()
    if not wl:
        print("横盘缩量监控列表为空")
        return
    print(f"横盘缩量监控列表 ({len(wl)}只):")
    for item in wl:
        print(f"  {item['code']} {item['name']} (添加于{item['added']})")


def check_sideway_watchlist():
    """批量检查横盘缩量监控列表"""
    wl = load_sideway_watchlist()
    if not wl:
        print("横盘缩量监控列表为空")
        return []

    print(f"横盘缩量监控 | {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"检查 {len(wl)} 只股票...\n")

    alerts = []
    for item in wl:
        code = item['code']
        name = item['name']

        r = check_sideway_single(code, name)
        print()

        if r is not None and r['signal']:
            alerts.append(f"🔔 {name}({code}) | {r['sideway_level']} + {r['shrink_level']} | "
                         f"振幅{r['amplitude']}% | 缩量至{r['avg_shrink_ratio']:.0%}")

    if alerts:
        print("=" * 60)
        print("⚠️ 横盘缩量信号触发:")
        for a in alerts:
            print(f"  {a}")
    else:
        print("暂无横盘+极致缩量完整信号（部分股票在横盘但未极致缩量，或反之）")

    return alerts


# ============================================================
# 格式化
# ============================================================
def format_report(r):
    """格式化单股报告"""
    if r is None:
        return "  非低位或无量能异动"

    lines = []
    code_name = f"{r.get('code','')} {r.get('name','')}".strip()
    lines.append(f"  {code_name} | {r['date']} | 收{r['close']}({r['pct_change']:+.2f}%)")
    lines.append(f"  量能: {r['vol_level']} {r['vol_desc']}")
    lines.append(f"  位置: {r['low_level']} (120日百分位{r['price_pct']}%)")
    lines.append(f"  均线: fast={r['cur_zf']} slow={r['cur_zs']} MA60={r['cur_ma60']}")
    lines.append(f"  fast-slow: {r['dist_fast_slow']:+.2f}% | 距MA60: {r['dist_ma60']:+.2f}% | KDJ.J={r['kdj_j']}")
    lines.append(f"  关注: {r['follow_path']}")
    if r['risk_tags']:
        lines.append(f"  风险: {'; '.join(r['risk_tags'])}")
    return '\n'.join(lines)


def format_brief(r):
    """格式化简报"""
    if r is None:
        return None
    code_name = f"{r.get('name','') or r.get('ts_code','') or r.get('code','')}"
    code = r.get('code', r.get('ts_code', ''))
    risk = f" [{'; '.join(r['risk_tags'])}]" if r['risk_tags'] else ""
    return (f"{code_name}({code}) {r['vol_level']} {r['low_level']} | "
            f"收{r['close']}({r['pct_change']:+.2f}%) | "
            f"量{r['vol_desc']} | "
            f"fast-slow{r['dist_fast_slow']:+.1f}% | "
            f"{r['follow_path']}{risk}")


# ============================================================
# 全量扫描 (tushare)
# ============================================================
def run_scan(limit=None):
    """全市场低位量能异动扫描"""
    # 1. 获取/加载全市场数据
    market_df = load_cached_market_daily()
    
    if market_df is None:
        print("无缓存数据，正在从tushare拉取全市场日线...")
        market_df = fetch_full_market_daily()
    
    if market_df is None:
        print("❌ 无法获取数据")
        return []
    
    # 检查数据日期
    latest_date = market_df['trade_date'].max()
    today = datetime.now().strftime("%Y%m%d")
    if latest_date < today:
        # 判断是否工作日
        weekday = datetime.now().weekday()
        if weekday < 5:  # 周一至周五
            print(f"⚠️ 数据最新日期 {latest_date}，非今日 {today}，建议运行 refresh")
    
    # 2. 获取股票基本信息
    pro = get_tushare_pro()
    try:
        stock_info = pro.stock_basic(exchange='', list_status='L', fields='ts_code,name,industry')
        info_map = {row['ts_code']: {'name': row['name'], 'industry': row['industry']} 
                    for _, row in stock_info.iterrows()}
    except:
        info_map = {}
    
    # 3. 按股票分组计算
    ts_codes = market_df['ts_code'].unique()
    if limit:
        ts_codes = ts_codes[:limit]
    
    print(f"\n低位量能异动扫描 | {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"扫描 {len(ts_codes)} 只股票 (数据截至 {latest_date})...\n")
    
    results = []
    total = len(ts_codes)
    
    for i, ts_code in enumerate(ts_codes):
        if (i + 1) % 500 == 0:
            print(f"  进度: {i + 1}/{total}")
        
        stock_df = market_df[market_df['ts_code'] == ts_code].sort_values('trade_date').reset_index(drop=True)
        
        if len(stock_df) < SCAN_MIN_HISTORY:
            continue
        
        close = stock_df['close'].values.astype(float)
        high = stock_df['high'].values.astype(float)
        low = stock_df['low'].values.astype(float)
        vol = stock_df['vol'].values.astype(float)  # tushare单位：手
        
        # 跳过数据异常
        if np.any(close <= 0) or np.any(vol < 0):
            continue
        
        info = info_map.get(ts_code, {})
        name = info.get('name', '')
        industry = info.get('industry', '')
        last_date = stock_df['trade_date'].iloc[-1]
        
        r = check_low_volume_anomaly_arrays(close, high, low, vol, last_date)
        if r is not None:
            r['code'] = ts_code
            r['name'] = name
            r['industry'] = industry
            r['ts_code'] = ts_code
            results.append(r)
            print(f"  >>> HIT: {ts_code} {name} {r['vol_level']} {r['low_level']} | "
                  f"收{r['close']} | {r['vol_desc']} | {r['follow_path']}")
    
    # 排序
    low_order = {'深度低位': 0, '相对低位': 1, '金叉前夕': 2, '刚金叉': 3, 'MA60附近': 4}
    vol_order = {'⚡⚡⚡': 0, '⚡⚡': 1, '⚡': 2}
    results.sort(key=lambda x: (low_order.get(x['low_level'], 5), vol_order.get(x['vol_level'], 3)))
    
    print(f"\n{'=' * 70}")
    print(f"低位量能异动结果 | 命中 {len(results)}/{total}")
    print(f"{'=' * 70}")
    
    if not results:
        print("今日无低位量能异动信号")
    else:
        # 按行业统计
        industry_count = {}
        for r in results:
            ind = r.get('industry', '未知')
            industry_count[ind] = industry_count.get(ind, 0) + 1
        top_industries = sorted(industry_count.items(), key=lambda x: -x[1])[:10]
        print(f"\n行业分布: {' | '.join(f'{k}:{v}' for k, v in top_industries)}")
        
        for i, r in enumerate(results):
            industry_tag = f"[{r.get('industry', '')}]" if r.get('industry') else ''
            code_name = f"{r['name'] or r['ts_code']}"
            risk = f" [{'; '.join(r['risk_tags'])}]" if r['risk_tags'] else ""
            print(f"\n[{i + 1}] {industry_tag} {code_name}({r['ts_code']}) {r['vol_level']} {r['low_level']} | "
                  f"收{r['close']}({r['pct_change']:+.2f}%) | "
                  f"量{r['vol_desc']} | "
                  f"fast-slow{r['dist_fast_slow']:+.1f}% | "
                  f"{r['follow_path']}{risk}")
    
    return results


# ============================================================
# Watchlist 管理
# ============================================================
def load_watchlist():
    if not os.path.exists(WATCHLIST_PATH):
        return []
    with open(WATCHLIST_PATH, 'r', encoding='utf-8') as f:
        return json.load(f)


def save_watchlist(wl):
    with open(WATCHLIST_PATH, 'w', encoding='utf-8') as f:
        json.dump(wl, f, ensure_ascii=False, indent=2)


def add_to_watchlist(code, name=""):
    wl = load_watchlist()
    if any(item['code'] == code for item in wl):
        print(f"{code} 已在监控列表中")
        return
    wl.append({"code": code, "name": name, "added": datetime.now().strftime('%Y-%m-%d')})
    save_watchlist(wl)
    print(f"✅ 已添加 {code} {name} 到监控列表")


def remove_from_watchlist(code):
    wl = load_watchlist()
    original_len = len(wl)
    wl = [item for item in wl if item['code'] != code]
    if len(wl) == original_len:
        print(f"{code} 不在监控列表中")
        return
    save_watchlist(wl)
    print(f"✅ 已从监控列表移除 {code}")


def list_watchlist():
    wl = load_watchlist()
    if not wl:
        print("监控列表为空")
        return
    print(f"量能异动监控列表 ({len(wl)}只):")
    for item in wl:
        print(f"  {item['code']} {item['name']} (添加于{item['added']})")


def check_watchlist():
    """批量检查监控列表"""
    wl = load_watchlist()
    if not wl:
        print("监控列表为空")
        return []

    print(f"低位量能异动监控 | {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"检查 {len(wl)} 只股票...\n")

    alerts = []
    for item in wl:
        code = item['code']
        name = item['name']

        df = fetch_kline(code)
        if df is None:
            print(f"  {code} {name} — 数据获取失败\n")
            continue

        r = check_low_volume_anomaly(df, code, name)
        print(format_report(r))
        print()

        if r is not None:
            alerts.append(format_brief(r))

    if alerts:
        print("=" * 60)
        print("⚠️ 低位异动信号:")
        for a in alerts:
            print(f"  {a}")
    else:
        print("监控列表中暂无低位量能异动信号")

    return alerts


def check_single(code, name=""):
    """单股低位异动诊断"""
    df = fetch_kline(code)
    if df is None:
        print(f"无法获取 {code} 的K线数据")
        return None

    r = check_low_volume_anomaly(df, code, name)
    print(format_report(r))
    return r


# ============================================================
# 命令行入口
# ============================================================
def main():
    parser = argparse.ArgumentParser(description="量能异动监控 v3.0 (tushare全市场)")
    subparsers = parser.add_subparsers(dest="command", help="子命令")

    # scan
    scan_parser = subparsers.add_parser("scan", help="全市场低位量能异动扫描")
    scan_parser.add_argument("--limit", type=int, default=None, help="限制扫描数量(调试用)")
    scan_parser.add_argument("--refresh", action="store_true", help="强制刷新数据")

    # check
    check_parser = subparsers.add_parser("check", help="单股低位异动诊断")
    check_parser.add_argument("code", help="股票代码 (如 sz002368)")
    check_parser.add_argument("--name", default="", help="股票名称")

    # watchlist
    subparsers.add_parser("watchlist", help="批量检查监控列表")

    # add
    add_parser = subparsers.add_parser("add", help="添加到监控列表")
    add_parser.add_argument("code", help="股票代码")
    add_parser.add_argument("name", nargs="?", default="", help="股票名称")

    # remove
    remove_parser = subparsers.add_parser("remove", help="从监控列表移除")
    remove_parser.add_argument("code", help="股票代码")

    # list
    subparsers.add_parser("list", help="查看监控列表")

    # refresh
    subparsers.add_parser("refresh", help="刷新tushare全市场数据缓存")

    # sideway - 单股横盘缩量诊断
    sideway_parser = subparsers.add_parser("sideway", help="单股横盘缩量诊断")
    sideway_parser.add_argument("code", help="股票代码 (如 sz300866)")
    sideway_parser.add_argument("--name", default="", help="股票名称")

    # sideway-add
    sideway_add_parser = subparsers.add_parser("sideway-add", help="添加到横盘缩量监控列表")
    sideway_add_parser.add_argument("code", help="股票代码")
    sideway_add_parser.add_argument("name", nargs="?", default="", help="股票名称")

    # sideway-remove
    sideway_remove_parser = subparsers.add_parser("sideway-remove", help="从横盘缩量监控列表移除")
    sideway_remove_parser.add_argument("code", help="股票代码")

    # sideway-list
    subparsers.add_parser("sideway-list", help="查看横盘缩量监控列表")

    # sideway-watchlist
    subparsers.add_parser("sideway-watchlist", help="批量检查横盘缩量监控列表")

    args = parser.parse_args()

    if args.command == "scan":
        if args.refresh:
            market_df = fetch_full_market_daily()
        run_scan(limit=args.limit)
    elif args.command == "check":
        check_single(args.code, args.name)
    elif args.command == "watchlist":
        check_watchlist()
    elif args.command == "add":
        add_to_watchlist(args.code, args.name)
    elif args.command == "remove":
        remove_from_watchlist(args.code)
    elif args.command == "list":
        list_watchlist()
    elif args.command == "refresh":
        fetch_full_market_daily()
    elif args.command == "sideway":
        check_sideway_single(args.code, args.name)
    elif args.command == "sideway-add":
        add_to_sideway_watchlist(args.code, args.name)
    elif args.command == "sideway-remove":
        remove_from_sideway_watchlist(args.code)
    elif args.command == "sideway-list":
        list_sideway_watchlist()
    elif args.command == "sideway-watchlist":
        check_sideway_watchlist()
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
