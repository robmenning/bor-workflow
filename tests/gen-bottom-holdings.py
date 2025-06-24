import pandas as pd
import numpy as np
from itertools import product
from scipy.optimize import lsq_linear

# === USER INPUTS ===

top_positions = [
    {'holding_name': 'Premium Brands Holdings Corporation', 'asset_type': 'Canadian Equities', 'sector': 'Consumer Staples', 'currency': 'CAD', 'market_value': 5939075.40},
    {'holding_name': 'Kinaxis Inc.', 'asset_type': 'Canadian Equities', 'sector': 'Information Technology', 'currency': 'CAD', 'market_value': 5344170.86},
    {'holding_name': 'Burford Capital Limited', 'asset_type': 'US Equities', 'sector': 'Financial Services', 'currency': 'USD', 'market_value': 4949229.50},
    {'holding_name': 'dentalcorp Holdings Ltd.', 'asset_type': 'Canadian Equities', 'sector': 'Health Care', 'currency': 'CAD', 'market_value': 4751630.32},
    {'holding_name': 'Trisura Group Ltd.', 'asset_type': 'Canadian Equities', 'sector': 'Insurance', 'currency': 'CAD', 'market_value': 4554031.14},
    {'holding_name': 'Sangoma Technologies Corporation', 'asset_type': 'Canadian Equities', 'sector': 'Information Technology', 'currency': 'CAD', 'market_value': 3959383.60},
    {'holding_name': 'Zillow Group, Inc.', 'asset_type': 'US Equities', 'sector': 'Information Technology', 'currency': 'USD', 'market_value': 3959383.60},
    {'holding_name': 'Kraken Robotics Inc.', 'asset_type': 'Canadian Equities', 'sector': 'Industrials', 'currency': 'CAD', 'market_value': 3761784.42},
    {'holding_name': 'PAR Technology Corporation', 'asset_type': 'US Equities', 'sector': 'Information Technology', 'currency': 'USD', 'market_value': 3662444.83},
    {'holding_name': 'Molina Healthcare, Inc.', 'asset_type': 'US Equities', 'sector': 'Health Care', 'currency': 'USD', 'market_value': 3365476.06},
]

total_net_assets = 98_984_590

asset_totals = {
    'Canadian Equities': 62_857_215.15,
    'US Equities': 26_626_870.31,
    'Cash': 6_928_921.30,
    'Other Assets': 2_573_600.34
}
sector_totals = {
    'Information Technology': 30_384_267.13,
    'Financial Services': 12_765_995.01,
    'Health Care': 11_977_130.39,
    'Industrials': 10_707_335.72,
    'Consumer Staples': 8_908_613.10,
    'Other Sectors': 7_423_844.25,
    'Cash': 6_928_921.30,
    'Real Estate': 5_345_168.00,
    'Insurance': 4_553_292.14
}
currency_totals = {
    'CAD': 63_553_998.78,
    'USD': 32_858_888.28,
    'Other': 2_573_600.34
}

# === END USER INPUTS ===

assets = list(asset_totals.keys())
sectors = list(sector_totals.keys())
currencies = list(currency_totals.keys())

all_combos = list(product(assets, sectors, currencies))
top10_combos = set((t['asset_type'], t['sector'], t['currency']) for t in top_positions)
hypo_combos = [c for c in all_combos if c not in top10_combos]

def build_indexer(keys, combos, pos=0):
    idx = {k: [] for k in keys}
    for i, c in enumerate(combos):
        idx[c[pos]].append(i)
    return idx

asset_idx = build_indexer(assets, hypo_combos, 0)
sector_idx = build_indexer(sectors, hypo_combos, 1)
currency_idx = build_indexer(currencies, hypo_combos, 2)

def subtract_top10_from_marginals(marginals, idx):
    marginals = marginals.copy()
    for t in top_positions:
        marginals[t[idx]] -= t['market_value']
    return marginals

asset_rem = subtract_top10_from_marginals(asset_totals, 'asset_type')
sector_rem = subtract_top10_from_marginals(sector_totals, 'sector')
currency_rem = subtract_top10_from_marginals(currency_totals, 'currency')

n = len(hypo_combos)
m = len(assets) + len(sectors) + len(currencies)
A = np.zeros((m, n))
b = np.zeros(m)

for i, a in enumerate(assets):
    for j in asset_idx[a]:
        A[i, j] = 1
    b[i] = asset_rem[a]
offset = len(assets)
for i, s in enumerate(sectors):
    for j in sector_idx[s]:
        A[offset + i, j] = 1
    b[offset + i] = sector_rem[s]
offset2 = offset + len(sectors)
for i, c in enumerate(currencies):
    for j in currency_idx[c]:
        A[offset2 + i, j] = 1
    b[offset2 + i] = currency_rem[c]

res = lsq_linear(A, b, bounds=(0, np.inf), lsmr_tol='auto', verbose=1)

rows = []
for t in top_positions:
    rows.append({
        'holding_name': t['holding_name'],
        'asset_type': t['asset_type'],
        'sector': t['sector'],
        'currency': t['currency'],
        'market_value': round(t['market_value'], 2),
        'wt': round(t['market_value'] / total_net_assets, 8)
    })
for i, (a, s, c) in enumerate(hypo_combos):
    mv = max(0, res.x[i])
    rows.append({
        'holding_name': f'Hypothetical Position {i+1}',
        'asset_type': a,
        'sector': s,
        'currency': c,
        'market_value': round(mv, 2),
        'wt': round(mv / total_net_assets, 8)
    })

df = pd.DataFrame(rows)
df.to_csv('tests/data/top-bottom-200-20250430.csv', index=False, float_format='%.8f', quoting=1)

print(df.head(15))
print(f"Sum of market_value: {df['market_value'].sum():,.2f}")
print(f"Sum of wt: {df['wt'].sum():.8f}")
print("Check asset, sector, currency subtotals:")
print(df.groupby('asset_type')['market_value'].sum())
print(df.groupby('sector')['market_value'].sum())
print(df.groupby('currency')['market_value'].sum())
