from log.logger import logger
from core import CcxtOrderManagement, FutureExecution, CcxtPortfolioManagement
from adapter.adapter import SignalTweetAdapter
from adapter.adapter import SignalTweetDownstream
from typing import List, Dict, Any
from datetime import datetime, timezone, timedelta
from discord_webhook import DiscordWebhook
from config import get_config

def main():
    # ======== check open_timestamp_utc for close signal ========

    order_manager: CcxtOrderManagement = CcxtOrderManagement()
    portfolio_manager: CcxtPortfolioManagement = CcxtPortfolioManagement()

    holding_positions: List[Dict[str, Any]] = portfolio_manager.get_positions_summary()
 
    current_time_utc: datetime = datetime.now(timezone.utc)

    for pos in holding_positions:
        if pos['open_timestamp_utc'] is not None:
            open_time_utc: datetime = datetime.fromisoformat(pos['open_timestamp_utc'])
            time_difference: timedelta = current_time_utc - open_time_utc
            if time_difference > timedelta(hours=72): 
                order_manager.close_position_by_symbol(pos['symbol'])  
                logger.info(f"Closed position for {pos['symbol']} as it has been open for {time_difference}.")
            else:
                logger.info(f"Position for {pos['symbol']} has been open for {time_difference}. Not closing yet.")

    # ======== Check signal to close and open position ========

    adapter: SignalTweetAdapter = SignalTweetAdapter()
    signals: List[SignalTweetDownstream] = adapter.get_signal(use_tp_sl=False, usdc_amount=15) 

    if not signals:
        logger.info("No signals generated.")  
        return
    else:
        logger.info(f"Generated {len(signals)} signals.") 
    holding_positions: List[Dict[str, Any]] = portfolio_manager.get_positions_summary()
    filtered_duplicated = portfolio_manager.drop_duplicate_signals(signals)
    logger.info(f"Filtered duplicated: {filtered_duplicated}")
    filtered_position_in_port = portfolio_manager.filter_out_position_in_portfolio(filtered_duplicated, holding_positions)
    logger.info(f"Filtered position in portfolio: {filtered_position_in_port}")
    signal_should_open, signal_should_close = portfolio_manager.categorize_signals(filtered_duplicated, holding_positions) 
    logger.info(f"Signal should open: {len(signal_should_open)}")
    logger.info(f"Signal should close: {len(signal_should_close)}")
    if signal_should_close:
        for sig in signal_should_close:
            order_manager.close_position_by_symbol(sig.symbol)
            logger.info(f"Have signal in opposite side, closed position for {sig.symbol}.")
        
    # ======== Check signal to open position ========
    
    MAX_ALLOWED_POSITIONS = 10
    positions_count = portfolio_manager.positions_count() 
    available_slots: int = MAX_ALLOWED_POSITIONS - positions_count

    signals_to_open_now: list[Any] = []

    if signal_should_open:
        if available_slots <= 0:
            logger.warning(f"Portfolio is full. Cannot open new positions. Current count: {positions_count}.")
        else: 
            if len(signal_should_open) > available_slots:
                logger.warning(f"There are {len(signal_should_open)} new signals, but only {available_slots} slot(s) available. "
                               f"Only the first {available_slots} signal(s) will be processed.")
                signals_to_open_now = signal_should_open[:available_slots]
            else:
                # There is enough space for all new signals.
                signals_to_open_now = signal_should_open

    # Proceed to open positions for the selected signals.
    executor: FutureExecution = FutureExecution()
    if signals_to_open_now:
        logger.info(f"Proceeding to open {len(signals_to_open_now)} new position(s).")
        notification_message: str = ""
        for sig in signals_to_open_now:
            try: 
                executor.execute_trade(
                    symbol=sig.symbol, 
                    side=sig.side, 
                    target_usdc_amount=sig.target_usdc_amount,
                    leverage=2,
                    stop_loss_price=sig.sl_price,
                    take_profit_price=sig.tp_price
                )
                notification_message += f"SUCCESS: Order placed for {sig.side.upper()} {sig.symbol} with amount {sig.target_usdc_amount} USDC.\n"
                logger.info(f"SUCCESS: Order placed for {sig.side.upper()} {sig.symbol} with amount {sig.target_usdc_amount} USDC.")

            except Exception as e:
                logger.error(f"FAILED to place order for {sig.symbol}. Reason: {e}")
        if notification_message:
            webhook = DiscordWebhook(url=get_config(["hyperliquid", "webhook_url"]), content=notification_message)
            webhook.execute()
        portfolio_manager = CcxtPortfolioManagement()
        positions = portfolio_manager.get_positions_summary()
        logger.info(f"Positions: {positions}")
    else:
        logger.info("No new positions will be opened in this cycle.")

if __name__ == "__main__":
    main()