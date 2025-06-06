import logging
from typing import List, Tuple, Optional, Any
from dataclasses import dataclass
from core.crawler_utils.utils import get_database
from log.logger import logger

import pandas as pd
import pytz
from datetime import datetime, timezone, timedelta
from core.data_management import MarketDataFetcher

@dataclass
class SignalTweetUpstream:
    author_username: str
    winrate: float
    tweet_id: Optional[str]
    tweet_text: Optional[str]
    tweet_created_at: Optional[datetime]
    ticker: Optional[str]
    action: Optional[str]
    message: Optional[str]

@dataclass
class SignalTweetDownstream:
    symbol: str
    side: str
    order_type: str
    target_usdc_amount: float | None 
    tp_price: float | None
    sl_price: float | None 
    

class SignalTweetAdapter:

    def __init__(self) -> None:
        self.author: list[str] = [
            "trading__horse", 
            "NB1763", 
            "Learnernoearner", 
            "CoinGurruu",
            "MustStopMurad",
            "aixbt_agent",
            "DarkCryptoLord",
            "jtradestar",
            "TheCryptoDog"
        ]

    def _query_signal_upstream(self) -> List[SignalTweetUpstream] | None:
        additional_where: str = (
            "twitter_crypto_backtesting.crypto_winrate_1d * 0.35 + "
            "twitter_crypto_backtesting.crypto_winrate_3d * 0.3 + "
            "twitter_crypto_backtesting.crypto_winrate_7d * 0.2 + "
            "twitter_crypto_backtesting.crypto_winrate_15d * 0.1 + "
            "twitter_crypto_backtesting.crypto_winrate_30d * 0.05"
        ) 
        query: str = f"""
            WITH ranked_authors AS (
                SELECT 
                    twitter_crypto_author_profile.author_id,
                    twitter_crypto_author_profile.author_username,
                    twitter_crypto_author_profile.author_url,
                    twitter_crypto_author_profile.author_twitterurl,
                    twitter_crypto_author_profile.author_name,
                    twitter_crypto_author_profile.author_followers,
                    twitter_crypto_author_profile.author_following,
                    twitter_crypto_author_profile.created_at,
                    twitter_crypto_author_profile.updated_at,
                    {additional_where} AS winrate,
                    ROW_NUMBER() OVER (ORDER BY {additional_where} DESC) AS rank
                FROM twitter_crypto_author_profile
                INNER JOIN twitter_crypto_backtesting
                    ON twitter_crypto_backtesting.author_id = twitter_crypto_author_profile.author_id
                WHERE twitter_crypto_author_profile.is_select = true 
                    AND twitter_crypto_backtesting.total_count_signals >= 10
                    AND twitter_crypto_author_profile.author_username IN ({', '.join([f"'{author}'" for author in self.author])})
            )
            SELECT 
                ra.author_username,
                ra.winrate,
                tct.id AS tweet_id,
                tct.text AS tweet_text,
                tct.created_at AS tweet_created_at,
                tcs.ticker,
                tcs.action,
                NULL AS message
            FROM ranked_authors ra
            LEFT JOIN twitter_crypto_tweets tct
                ON ra.author_username = tct.author_username
            LEFT JOIN twitter_crypto_signal tcs
                ON tct.id = tcs.tweet_id
            ORDER BY ra.winrate DESC, tct.created_at DESC
        """

        db: Any = get_database()
        try:
            with db.cursor() as cur:
                cur.execute(query)
                rows: List[Tuple[Any, ...]] = cur.fetchall()
                # ---- Filter by UTC hour in Python ----
                bangkok: Any = pytz.timezone('Asia/Bangkok')
                now_utc: datetime = datetime.now(timezone.utc)
                hour_start_utc: datetime = now_utc.replace(minute=0, second=0, microsecond=0)
                hour_end_utc: datetime = hour_start_utc + timedelta(hours=1)
                filtered_rows: list[tuple[Any, ...]] = []
                for row in rows:
                    tweet_created_at = row[4]  # Index 4 is tweet_created_at
                    
                    if tweet_created_at is None:
                        continue
                    
                    # Ensure tweet_created_at is timezone-aware in Bangkok
                    if tweet_created_at.tzinfo is None:
                        
                        tweet_created_at = bangkok.localize(tweet_created_at)
                    tweet_created_at_utc: datetime = tweet_created_at.astimezone(timezone.utc)
                    
                    if hour_start_utc <= tweet_created_at_utc < hour_end_utc:
                        filtered_rows.append(row)
                return [SignalTweetUpstream(*row) for row in filtered_rows]
        except Exception as e:
            logger.error(f"ERROR::_query_signal_upstream(): {e}") 
            if db:
                db.rollback()
            return []
        finally:
            if db:
                db.close()

    @staticmethod
    def calculate_atr(df: pd.DataFrame, period: int = 20) -> float | None:
        """
        Calculates the Average True Range (ATR) for a given DataFrame.

        Args:
            df (pd.DataFrame): DataFrame with 'high', 'low', and 'close' columns.
                            It's assumed the DataFrame's index is a DatetimeIndex
                            if 'Previous Close' needs to be derived accurately.
            period (int): The period (window) for ATR calculation. Defaults to 20.

        Returns:
            float: The last ATR value.
                    Returns None if input is invalid or calculation fails.
        """
        if not isinstance(df, pd.DataFrame):
            logger.error("Error: Input 'df' must be a Pandas DataFrame.")
            return None
        if not all(col in df.columns for col in ['high', 'low', 'close']):
            logger.error("Error: DataFrame must contain 'high', 'low', and 'close' columns.")
            return None
        if not isinstance(period, int) or period <= 0:
            logger.error("Error: 'period' must be a positive integer.")
            return None

        try:
            high_low: pd.Series = df['high'] - df['low']
            high_prev_close: pd.Series = abs(df['high'] - df['close'].shift(1))
            low_prev_close: pd.Series = abs(df['low'] - df['close'].shift(1))
            tr_components: pd.DataFrame = pd.concat([high_low, high_prev_close, low_prev_close], axis=1)
            true_range: pd.Series = tr_components.max(axis=1, skipna=False)
            atr: pd.Series = true_range.ewm(alpha=1/period, adjust=False, min_periods=period).mean()
            return round(atr.iloc[-1], 2)

        except Exception as e:
            logger.error(f"ERROR::calculate_atr(): {e}")
            return None

    def _downstream_calculating(self, signal_tweets: List[SignalTweetUpstream], timeframe: str = '4h', usdc_amount: float = 15, use_tp_sl: bool = False) -> List[SignalTweetDownstream]:
        """
        Calculates downstream signals based on upstream signals.

        Args:
            signal_tweets (List[SignalTweetUpstream]): List of upstream signals.
            timeframe (str): Timeframe for OHLCV data. Defaults to '4h'.
            usdc_amount (float): Amount of USDC to trade. Defaults to 15.
            use_tp_sl (bool): Whether to use Take Profit and Stop Loss. Defaults to False.

        Returns:
            List[SignalTweetDownstream]: List of downstream signals.
        """
        market_data_fetcher = MarketDataFetcher()
        downstream_signals: List[SignalTweetDownstream] = []

        if not signal_tweets:
            logger.warning("No upstream signals provided. Returning empty downstream signals list.")
            return []
        
        for signal_tweet in signal_tweets:
            if not signal_tweet.action:
                logger.warning(f"Skipping signal due to missing or invalid action: {signal_tweet}")
                continue
            
            
            action_map = {
                "long": "buy",
                "short": "sell",
            }
            normalized_action_input = signal_tweet.action.lower()
            action = action_map.get(normalized_action_input)

            if action is None:
                # logger.warning(f"Unknown action '{signal_tweet.action}'. Skipping signal.")
                continue  # Skip this signal_tweet and proceed with the next one
            if not signal_tweet.ticker:
                logger.warning(f"Skipping signal due to missing ticker: {signal_tweet}")
                continue
            logger.info(f"===========created_at: {signal_tweet.tweet_created_at}===============")
            logger.info(f"signal_tweet: {signal_tweet}")
            # Clean ticker format: BTCUSDT -> BTC/USDC:USDC
            cleaned_ticker = signal_tweet.ticker.replace('USDT', '/USDC:USDC')

            if not use_tp_sl:
                downstream_signals.append(
                    SignalTweetDownstream(
                        symbol=cleaned_ticker,
                        side=action,
                        order_type="market",
                        target_usdc_amount=usdc_amount,
                        tp_price=None,
                        sl_price=None
                    )
                )
                continue

            ohlcv_df = market_data_fetcher.get_ohlcv_df(symbol=cleaned_ticker, timeframe=timeframe, limit=100)

            if ohlcv_df is None or ohlcv_df.empty:
                logger.warning(f"Could not fetch OHLCV data for {cleaned_ticker}. Skipping TP/SL calculation.")
                downstream_signals.append(
                    SignalTweetDownstream(
                        symbol=cleaned_ticker,
                        side=action,  # Use mapped action
                        order_type="market",
                        target_usdc_amount=usdc_amount,  # Set usdc_amount as TP/SL failed
                        tp_price=None,
                        sl_price=None
                    )
                )
                continue

            latest_close_price = ohlcv_df['close'].iloc[-1]
            atr_value = self.calculate_atr(ohlcv_df)

            tp_price: float | None = None
            sl_price: float | None = None

            # Use the mapped 'action' for TP/SL logic
            if atr_value is not None:
                if action == 'buy':  # Corresponds to 'long'
                    tp_price = latest_close_price + (2 * atr_value)
                    sl_price = latest_close_price - (1.5 * atr_value)
                elif action == 'sell':  # Corresponds to 'short'
                    tp_price = latest_close_price - (2 * atr_value)
                    sl_price = latest_close_price + (1.5 * atr_value)
            
            downstream_signals.append(
                SignalTweetDownstream(
                    symbol=cleaned_ticker,
                    side=action,  # Use mapped action
                    order_type="market",
                    target_usdc_amount=usdc_amount,
                    tp_price=tp_price,
                    sl_price=sl_price
                )
            )
        return downstream_signals 

    def get_signal(self, use_tp_sl: bool = False, usdc_amount: float = 15, timeframe: str = '4h') -> List[SignalTweetDownstream]:
        """
        Returns a list of downstream signals based on upstream signals.

        Args:
            use_tp_sl (bool): Whether to use Take Profit and Stop Loss. Defaults to False.
            usdc_amount (float): Amount of USDC to trade. Defaults to 15.
            timeframe (str): Timeframe for OHLCV data. Defaults to '4h'.

        Returns:
            List[SignalTweetDownstream]: List of downstream signals.
        """
        results: List[SignalTweetUpstream] | None = self._query_signal_upstream() 
        downstream_signals: List[SignalTweetDownstream] | None = self._downstream_calculating(results, use_tp_sl=use_tp_sl, usdc_amount=usdc_amount, timeframe=timeframe)
        return downstream_signals


# Example mock to test _downstream_calculating
# from dataclasses import dataclass
# from typing import Optional

# @dataclass
# class SignalTweetUpstream:
#     author_username: str
#     winrate: float
#     tweet_id: Optional[str]
#     tweet_text: Optional[str]
#17:     tweet_created_at: Optional[datetime]
#     ticker: Optional[str]
#     action: Optional[str]
#     message: Optional[str]

# results = [
#     SignalTweetUpstream(author_username='dunstonlol', winrate=88.0, tweet_id=None, tweet_text=None, tweet_created_at=None, ticker='ETHUSDT', action='LONG', message=None),
#     SignalTweetUpstream(author_username='DeribitInsights', winrate=77.501, tweet_id=None, tweet_text=None, tweet_created_at=None, ticker='BTCUSDT', action='LONG', message=None),
#     SignalTweetUpstream(author_username='owen1v9', winrate=77.0, tweet_id=None, tweet_text=None, tweet_created_at=None, ticker='BTCUSDT', action='NONE', message=None),
#     SignalTweetUpstream(author_username='2laxar', winrate=76.5, tweet_id=None, tweet_text=None, tweet_created_at=None, ticker='TAOUSDT', action='NONE', message=None),
#     SignalTweetUpstream(author_username='kelxyz_', winrate=71.81849999999999, tweet_id=None, tweet_text=None, tweet_created_at=None, ticker='IMXUSDT', action='SHORT', message=None),
#     SignalTweetUpstream(author_username='CoinGurruu', winrate=71.6905, tweet_id=None, tweet_text=None, tweet_created_at=None, ticker='KAIQUSDT', action='EXIT_LONG', message=None),  
#     SignalTweetUpstream(author_username='jimtalbot', winrate=70.622, tweet_id=None, tweet_text=None, tweet_created_at=None, ticker=None, action=None, message=None),
#     SignalTweetUpstream(author_username='Gunzjbass89', winrate=70.0, tweet_id=None, tweet_text=None, tweet_created_at=None, ticker=None, action=None, message=None),
#     SignalTweetUpstream(author_username='fejau_inc', winrate=69.6875, tweet_id=None, tweet_text=None, tweet_created_at=None, ticker=None, action=None, message=None),
#     SignalTweetUpstream(author_username='chameleonsunday', winrate=69.5, tweet_id=None, tweet_text=None, tweet_created_at=None, ticker=None, action=None, message=None)
# ]
# from core.ccxt_hyperliquid.adapter.adapter import SignalTweetAdapter 

# adapter: SignalTweetAdapter = SignalTweetAdapter()
# signals = adapter._downstream_calculating(results, use_tp_sl=False, usdc_amount=15, timeframe='4h')