"""
CFTC 持仓数据抓取 (精简版, 用于 GitHub Actions)
输出 JSON 摘要到 data/cftc/
"""

import pandas as pd
import numpy as np
import requests
from datetime import datetime, timedelta
import json
import os
import warnings
warnings.filterwarnings('ignore')

CFTC_TFF_URL = "https://publicreporting.cftc.gov/resource/gpe5-46if.json"
CFTC_DISAGG_URL = "https://publicreporting.cftc.gov/resource/72hh-3qpy.json"
LOOKBACK_DAYS = 1200
ZSCORE_WINDOW = 156

TFF_CONTRACTS = [
    {'name': 'S&P 500', 'cftc': 'E-MINI S&P 500 -', 'section': 'Equity'},
    {'name': 'Nasdaq 100', 'cftc': 'NASDAQ MINI', 'section': 'Equity'},
    {'name': 'Russell 2000', 'cftc': 'RUSSELL E-MINI', 'section': 'Equity'},
    {'name': 'UST 2Y', 'cftc': 'UST 2Y NOTE', 'section': 'Bonds'},
    {'name': 'UST 10Y', 'cftc': 'UST 10Y NOTE', 'section': 'Bonds'},
    {'name': 'EUR/USD', 'cftc': 'EURO FX - CHICAGO', 'section': 'FX'},
    {'name': 'JPY/USD', 'cftc': 'JAPANESE YEN', 'section': 'FX'},
    {'name': 'GBP/USD', 'cftc': 'BRITISH POUND', 'section': 'FX'},
    {'name': 'Bitcoin', 'cftc': 'BITCOIN - CHICAGO MERCANTILE', 'section': 'FX'},
]

DISAGG_CONTRACTS = [
    {'name': 'WTI Crude', 'cftc': 'WTI-PHYSICAL', 'section': 'Energy'},
    {'name': 'Natural Gas', 'cftc': 'NAT GAS NYME', 'section': 'Energy'},
    {'name': 'Gold', 'cftc': 'GOLD - COMMODITY', 'section': 'Metals'},
    {'name': 'Silver', 'cftc': 'SILVER - COMMODITY', 'section': 'Metals'},
    {'name': 'Copper', 'cftc': 'COPPER- #1', 'section': 'Metals'},
]


def fetch_cftc(endpoint, start_date):
    params = {
        "$where": f"report_date_as_yyyy_mm_dd >= '{start_date}'",
        "$limit": 50000,
        "$order": "report_date_as_yyyy_mm_dd ASC"
    }
    resp = requests.get(endpoint, params=params, timeout=120)
    resp.raise_for_status()
    df = pd.DataFrame(resp.json())
    if df.empty:
        return df
    skip = {'market_and_exchange_names', 'report_date_as_yyyy_mm_dd',
            'cftc_contract_market_code', 'cftc_market_code', 'commodity',
            'contract_units', 'futonly_or_combined', 'id',
            'commodity_name', 'commodity_group_name', 'commodity_subgroup_name'}
    for col in df.columns:
        if col not in skip:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    df['report_date'] = pd.to_datetime(df['report_date_as_yyyy_mm_dd'])
    return df


def match_cftc(df, pattern):
    if not pattern:
        return None
    mask = df['market_and_exchange_names'].str.contains(pattern, case=False, na=False)
    matched = df[mask].copy()
    if matched.empty:
        return None
    if matched['market_and_exchange_names'].nunique() > 1:
        avg_oi = matched.groupby('market_and_exchange_names')['open_interest_all'].mean()
        matched = matched[matched['market_and_exchange_names'] == avg_oi.idxmax()]
    return matched.sort_values('report_date').reset_index(drop=True)


def calc_zscore(series, window=ZSCORE_WINDOW):
    s = series.dropna()
    if len(s) < 10:
        return None
    tail = s.tail(window)
    mean, std = tail.mean(), tail.std()
    if std == 0 or np.isnan(std):
        return 0.0
    return round(float((s.iloc[-1] - mean) / std), 2)


def analyze(matched, long_col, short_col):
    long_s = matched[long_col].fillna(0)
    short_s = matched[short_col].fillna(0)
    net_s = long_s - short_s
    oi = matched['open_interest_all'].fillna(0).replace(0, np.nan)
    net_oi = net_s / oi

    result = {
        'net': int(net_s.iloc[-1]),
        'net_z': calc_zscore(net_oi),
        'net_change': int(net_s.diff().iloc[-1]) if len(net_s) > 1 else 0,
        'long': int(long_s.iloc[-1]),
        'short': int(short_s.iloc[-1]),
    }

    # Flow state
    z_dl = calc_zscore(long_s.diff().dropna()) if len(long_s) > 10 else None
    z_ds = calc_zscore(short_s.diff().dropna()) if len(short_s) > 10 else None
    result['flow'] = _flow(z_dl, z_ds)
    result['crowding'] = _crowding(result['net_z'])
    return result


def _flow(zl, zs):
    if zl is None or zs is None:
        return ''
    if zl >= 0.8 and zs <= -0.8: return 'Long Squeeze'
    if zl <= -0.8 and zs >= 0.8: return 'Short Pressure'
    if zl >= 0.8 and abs(zs) < 0.5: return 'Long Build'
    if zs >= 0.8 and abs(zl) < 0.5: return 'Short Build'
    if zl <= -0.8 and abs(zs) < 0.5: return 'Long Unwind'
    if zs <= -0.8 and abs(zl) < 0.5: return 'Short Cover'
    return ''


def _crowding(net_z):
    if net_z is None:
        return ''
    if net_z >= 2.75: return 'EXTREME LONG'
    if net_z >= 2.0: return 'Crowded Long'
    if net_z <= -2.75: return 'EXTREME SHORT'
    if net_z <= -2.0: return 'Crowded Short'
    return ''


def main():
    start_date = (datetime.now() - timedelta(days=LOOKBACK_DAYS)).strftime('%Y-%m-%d')

    print("Fetching TFF data...")
    df_tff = fetch_cftc(CFTC_TFF_URL, start_date)
    print(f"  {len(df_tff)} rows")

    print("Fetching Disagg data...")
    df_disagg = fetch_cftc(CFTC_DISAGG_URL, start_date)
    print(f"  {len(df_disagg)} rows")

    report_date = df_tff['report_date'].max().strftime('%Y-%m-%d')
    print(f"Report date: {report_date}")

    results = {"report_date": report_date, "generated": datetime.now().isoformat(), "data": []}

    for c in TFF_CONTRACTS:
        matched = match_cftc(df_tff, c['cftc'])
        if matched is None:
            continue
        row = analyze(matched, 'lev_money_positions_long', 'lev_money_positions_short')
        row['name'] = c['name']
        row['section'] = c['section']
        row['type'] = 'Leveraged Funds'
        results['data'].append(row)

    for c in DISAGG_CONTRACTS:
        matched = match_cftc(df_disagg, c['cftc'])
        if matched is None:
            continue
        row = analyze(matched, 'm_money_positions_long_all', 'm_money_positions_short_all')
        row['name'] = c['name']
        row['section'] = c['section']
        row['type'] = 'Managed Money'
        results['data'].append(row)

    # ── 写入 ──
    os.makedirs("data/cftc", exist_ok=True)
    json_path = f"data/cftc/{report_date}.json"
    with open(json_path, "w") as f:
        json.dump(results, f, indent=2)
    with open("data/cftc/latest.json", "w") as f:
        json.dump(results, f, indent=2)

    # ── 同时生成可读的 markdown ──
    md_lines = [f"# CFTC Positioning — {report_date}\n"]
    for section in ['Equity', 'Bonds', 'FX', 'Energy', 'Metals']:
        items = [d for d in results['data'] if d['section'] == section]
        if not items:
            continue
        md_lines.append(f"## {section}")
        md_lines.append("| Asset | Net | Z-Score | W/W Chg | Flow | Crowding |")
        md_lines.append("|-------|-----|---------|---------|------|----------|")
        for d in items:
            z = f"{d['net_z']:.1f}" if d['net_z'] is not None else "—"
            md_lines.append(
                f"| {d['name']} | {d['net']:,} | {z} | {d['net_change']:+,} | {d['flow']} | {d['crowding']} |"
            )
        md_lines.append("")

    md_path = f"data/cftc/{report_date}.md"
    with open(md_path, "w") as f:
        f.write("\n".join(md_lines))
    with open("data/cftc/latest.md", "w") as f:
        f.write("\n".join(md_lines))

    print(f"✅ {json_path}")
    print(f"✅ {md_path}")


if __name__ == "__main__":
    main()
