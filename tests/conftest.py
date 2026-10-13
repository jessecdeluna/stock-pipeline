import pytest

# 2026-10-09 15:59 New York time
MARKET_TS = 1791575940


def screener_quote(symbol, open_=100.0, avg_vol=1_000_000, pe=20.0, **extra):
    q = {
        "symbol": symbol,
        "shortName": f"{symbol} Inc.",
        "regularMarketOpen": open_,
        "averageDailyVolume3Month": avg_vol,
        "regularMarketVolume": avg_vol * 2,
        "regularMarketTime": MARKET_TS,
    }
    if pe is not None:
        q["trailingPE"] = pe
    q.update(extra)
    return q


@pytest.fixture
def sample_quotes():
    return [
        screener_quote("NVDA", 221.59, 151_325_148, 34.24),
        screener_quote("TSLA", 322.33, 45_302_554, 304.24),
        screener_quote("AAPL", 311.32, 56_809_803, 35.97),
        screener_quote("INTC", 102.34, 123_393_238, None),  # negative earnings -> no P/E
        screener_quote("BAC", 62.71, 35_801_441, 14.59),
    ]
