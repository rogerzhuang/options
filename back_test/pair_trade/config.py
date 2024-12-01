# Configuration settings
PAIRS = [
    {'ticker1': 'MSTR', 'ticker2': 'BTC-USD'},
    {'ticker1': 'AMAT', 'ticker2': 'LRCX'},
    {'ticker1': 'NVDA', 'ticker2': 'AMD'},
    {'ticker1': 'LCID', 'ticker2': 'RIVN'},
    {'ticker1': 'ENPH', 'ticker2': 'SEDG'}
]

# Trading parameters
DEFAULT_PARAMS = {
    'z_score_window': 45,
    'z_score_threshold': 1.25,
    'correlation_threshold': 0.5,
    'max_position': 1,
    'vol_premium_multiplier': 0.015
}
