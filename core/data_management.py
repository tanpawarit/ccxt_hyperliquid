import ccxt
import asyncio
from typing import Optional, List, Any
from core.ccxt_hyperliquid.ccxt_base import CcxtBase
from core.ccxt_hyperliquid.log.logger import logger
import pandas as pd

class MarketDataFetcher(CcxtBase):
    """A class to fetch market data using the CCXT library."""
    def __init__(self) -> None:
        """
        Initializes the MarketDataFetcher by calling the parent CcxtBase
        constructor, which ensures the CCXT Hyperliquid exchange is initialized.
        """
        super().__init__()
        # Ensure the exchange is available before proceeding with order operations
        if not self.exchange:
            logger.warning("Hyperliquid exchange not initialized. Order operations may fail.") 
            
    def _fetch_ohlcv_timeseries(self,
                                 symbol: str,
                                 timeframe: str = '1m',
                                 since: Optional[int] = None,
                                 limit: Optional[int] = None) -> List[List] | None:
        """
        Fetches historical OHLCV data.
        """
        if not self.exchange:
            logger.error("Exchange not initialized.")
            return None

        if not self.exchange.has['fetchOHLCV']:
            logger.error(f"{self.exchange.id} does not support fetchOHLCV.")
            return None

        try:
            logger.info(f"Fetching OHLCV for {symbol} (Timeframe: {timeframe})...")
            
            if timeframe not in self.exchange.timeframes:
                logger.error(f"Timeframe '{timeframe}' not supported by {self.exchange.id}.")
                logger.warning(f"Supported timeframes: {list(self.exchange.timeframes.keys())}")
                return None

            ohlcv_data: List[List] = self.exchange.fetch_ohlcv(symbol, timeframe, since, limit)
            logger.info(f"Successfully fetched {len(ohlcv_data)} candles.")
            return ohlcv_data

        except Exception as e:
            logger.error(f"Error fetching OHLCV for {symbol}: {e}")
            return None

    def _format_ohlcv_data(self, ohlcv_data: List[List]) -> pd.DataFrame:
        columns: list[str] = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
        df: pd.DataFrame = pd.DataFrame(ohlcv_data, columns=columns)
        df['datetime'] = pd.to_datetime(df['timestamp'], unit='ms')
        df.set_index('datetime', inplace=True)
        return df

    def get_ohlcv_df(self,
                    symbol: str,
                    timeframe: str = '1m',
                    since: Optional[int] = None,
                    limit: Optional[int] = None) -> pd.DataFrame | None:
        ohlcv_data: List[List] | None = self._fetch_ohlcv_timeseries(symbol, timeframe, since, limit)
        if ohlcv_data is None:
            return None
        return self._format_ohlcv_data(ohlcv_data)

 