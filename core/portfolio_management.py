from ccxt_base import CcxtBase
from core import CcxtOrderManagement, CcxtWalletManagement
from log.logger import logger
from typing import Any, List, Dict
from collections import defaultdict
from datetime import datetime, timezone
from adapter.adapter import SignalTweetDownstream
from collections import defaultdict, Counter
class CcxtPortfolioManagement(CcxtBase):
    """
    Manages portfolio operations for Hyperliquid, including position count checks and summaries.
    All actions related to portfolio management are logged.
    """

    def __init__(self):
        self.order_manager: CcxtOrderManagement = CcxtOrderManagement()
        self.wallet_manager: CcxtWalletManagement = CcxtWalletManagement()
        if not self.order_manager.exchange or not self.wallet_manager.exchange:
            logger.warning("Hyperliquid exchange not initialized. Portfolio operations may fail.")

    def _get_positions(self) -> List[dict[str, Any]]:
        """Fetch all open positions using the order manager."""
        if not self.order_manager.exchange:
            logger.error("Exchange not initialized. Cannot fetch positions.")
            return []
        try:
            positions = self.order_manager.fetch_positions()
            logger.info(f"Fetched {len(positions)} open positions.")
            return positions
        except Exception as e:
            logger.error(f"Error fetching positions: {e}")
            return []

    def get_positions_summary(self) -> List[Dict[str, Any]]:
        """Return a summary of current open positions (symbol, size, side, entry price, open_timestamp_utc)."""
        if not self.order_manager or not self.order_manager.exchange:
            logger.error("Order manager or exchange not initialized. Cannot fetch trades for position timestamps.")
            positions_data = self._get_positions()
            summary = []
            for pos in positions_data:
                summary.append({
                    'symbol': pos.get('symbol'),
                    'size': pos.get('contracts') or pos.get('positionAmt') or pos.get('amount'),
                    'side': pos.get('side'),
                    'entry_price': pos.get('entryPrice') or pos.get('avgEntryPrice'),
                    'open_timestamp_utc': None
                })
            logger.warning("Returning positions summary without open timestamps as exchange is not available.")
            return summary

        positions = self._get_positions()
        summary = []
        for pos in positions:
            symbol = pos.get('symbol')
            side = pos.get('side')
            entry_price = pos.get('entryPrice') or pos.get('avgEntryPrice')
            contracts = pos.get('contracts') or pos.get('positionAmt') or pos.get('amount')
            open_timestamp_ms = None # Timestamp in milliseconds
            open_timestamp_utc = None # Timestamp in ISO 8601 UTC format

            if not symbol:
                logger.warning(f"Position data is missing 'symbol': {pos}")
                summary.append({
                    'symbol': None,
                    'size': contracts,
                    'side': side,
                    'entry_price': entry_price,
                    'open_timestamp_utc': None
                })
                continue

            try:
                trades = self.order_manager.exchange.fetchMyTrades(symbol=symbol, limit=100)
                position_side_for_trade = 'buy' if side == 'long' else 'sell'
                candidate_trades = [
                    t for t in trades
                    if t.get('symbol') == symbol and t.get('side') == position_side_for_trade
                ]

                if candidate_trades:
                    oldest_relevant_trade = candidate_trades[-1]
                    open_timestamp_ms = oldest_relevant_trade.get('timestamp')
                    if open_timestamp_ms is not None:
                        # Convert milliseconds to seconds for datetime.fromtimestamp
                        open_timestamp_utc = datetime.fromtimestamp(open_timestamp_ms / 1000, tz=timezone.utc).isoformat()

            except Exception as e:
                logger.error(f"Error fetching trades or determining open timestamp for position {symbol}: {e}")

            summary.append({
                'symbol': symbol,
                'size': contracts,
                'side': side,
                'entry_price': entry_price,
                'open_timestamp_utc': open_timestamp_utc # Store ISO string or None
            })
        
        logger.info(f"Successfully generated positions summary with best-effort open UTC timestamps for {len(summary)} positions.")
        return summary

    def positions_count(self) -> int:
        """Return the number of open positions."""
        return len(self._get_positions())
    
    @staticmethod
    def filter_out_position_in_portfolio(signals: list[SignalTweetDownstream], positions: list[dict]) -> list[SignalTweetDownstream]:
        """Filter out signals that have positions in portfolio. (duplicate symbol and side)"""
        remaining_signals = []
        existing_positions_set = set()
        for pos in positions: 
            normalized_side = ''
            if pos['side'] == 'long':
                normalized_side = 'buy'
            elif pos['side'] == 'short':
                normalized_side = 'sell'
            
            existing_positions_set.add((pos['symbol'], normalized_side))

        for sig in signals: 
            if (sig.symbol, sig.side) not in existing_positions_set:
                remaining_signals.append(sig)
        
        return remaining_signals

    @staticmethod
    def drop_duplicate_signals(signals: list[SignalTweetDownstream]) -> list[SignalTweetDownstream]:
        """
        Drop duplicate signals based on symbol and side.
        - For each symbol, keep only the first signal of the majority side (buy/sell).
        - If there is a tie in side counts for a symbol, drop all signals for that symbol.
        """
        if not signals:
            return []
        # Group signals by symbol
        signals_by_symbol: defaultdict[str, list[SignalTweetDownstream]] = defaultdict(list)
        for signal in signals:
            signals_by_symbol[signal.symbol].append(signal)

        final_signals: list[SignalTweetDownstream] = []

        for symbol, symbol_signals in signals_by_symbol.items():
            side_counts = Counter(signal.side for signal in symbol_signals)
            if len(side_counts) == 1:
                # Only one side, keep the first
                final_signals.append(symbol_signals[0])
            else:
                # More than one side, check for majority
                most_common = side_counts.most_common()
                if len(most_common) >= 2 and most_common[0][1] == most_common[1][1]:
                    # Tie, skip this symbol
                    continue
                # Keep the first signal of the majority side
                majority_side = most_common[0][0]
                for signal in symbol_signals:
                    if signal.side == majority_side:
                        final_signals.append(signal)
                        break

        return final_signals

    @staticmethod
    def categorize_signals(
        signal_list: list[dict | Any], 
        positions_list: list[dict[str, Any]]
    ) -> tuple[list[dict | Any], list[dict | Any]]:
        """
        Categorizes signals into open signals and close signals based on existing positions.

        Args:
            signal_list: List of signals (can be a list of objects or list of dicts)
            positions_list: List of positions (list of dicts)

        Returns:
            tuple: (open_signals, close_signals)
                open_signals (list): List of signals for opening new orders
                close_signals (list): List of signals for closing existing positions
        """
        open_signal_result: list[dict | Any] = []
        close_signal_result: list[dict | Any] = []

        for s_item in signal_list:
            is_closing_signal: bool = False
             
            # Extract symbol and side from signal (handling both dict and object)
            signal_symbol: str = ''
            signal_side: str = ''
            if isinstance(s_item, dict):
                signal_symbol = s_item.get('symbol', '')
                signal_side = s_item.get('side', '')
            else:  
                signal_symbol = getattr(s_item, 'symbol', '')
                signal_side = getattr(s_item, 'side', '')
            
            # Check if this signal would close any existing position
            for p_item in positions_list:
                position_symbol: str = p_item.get('symbol', '')
                position_side: str = p_item.get('side', '')
                
                if signal_symbol == position_symbol:
                    if (signal_side == 'buy' and position_side == 'short') or \
                       (signal_side == 'sell' and position_side == 'long'):
                        is_closing_signal = True
                        break 
            
            # Categorize the signal
            if is_closing_signal:
                close_signal_result.append(s_item)
            else:
                open_signal_result.append(s_item)
                
        return open_signal_result, close_signal_result