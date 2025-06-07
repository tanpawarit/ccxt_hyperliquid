from log.logger import logger
from core import CcxtOrderManagement, CcxtWalletManagement
import math
from typing import Any, Literal
# Custom Exceptions
class MarketNotActiveError(Exception): pass
class MarketInfoError(Exception): pass
class TickerFetchError(Exception): pass
class WalletBalanceError(Exception): pass
class DependentOrderError(Exception): pass # New custom exception

class FutureExecution:
    def __init__(self) -> None:
        self.order_manager: CcxtOrderManagement = CcxtOrderManagement()
        self.wallet_manager: CcxtWalletManagement = CcxtWalletManagement() 

    def _log_order_summary(self, order_name: str, order_result: dict[str, Any] | None) -> None:
        if order_result and isinstance(order_result, dict):
            info = order_result.get('info', {})
            status = order_result.get('status')
            if not status and isinstance(info, dict): # Hyperliquid often nests useful status info
                if info.get('filled'):
                    status = 'filled'
                elif info.get('resting'):
                    status = 'resting'
                elif info.get('error'):
                    status = f"error ({info.get('error')})"
            
            summary = {
                "id": order_result.get('id'),
                "status": status,
                "type": order_result.get('type'),
                "side": order_result.get('side'),
                "amount": order_result.get('amount'),
                "price": order_result.get('price'), # For limit/stop orders
                "average": order_result.get('average'), # For filled market orders
                "triggerPrice": order_result.get('triggerPrice') # For stop orders
            }
            # Clean None values for brevity
            summary_cleaned = {k: v for k, v in summary.items() if v is not None}
            logger.info(f"{order_name} summary: {summary_cleaned}")
        elif order_result:
            logger.info(f"{order_name}: {order_result}") # Log as is if not a dict or empty
        else:
            logger.info(f"{order_name}: Not placed or no result.")

    def _usdc_to_base_amount(
        self,
        target_usdc_amount: float, # Changed from float | None
        current_price: float,
        market_info: dict,
        min_viable_base_amount: float,
        symbol: str
    ) -> float:
        """
        Convert target_usdc_amount (in USDC) to base asset amount, adjusting for precision and minimum viable amount.
        Assumes target_usdc_amount is a positive float.
        """
        desired_base_amount: float = target_usdc_amount / current_price
        amount_precision: float = market_info['precision']['amount']
        adjusted_desired_base_amount: float

        if isinstance(amount_precision, (int, float)) and amount_precision > 0:
            if amount_precision < 1:  # Decimal precision (e.g., 0.01)
                decimal_places: int = abs(math.floor(math.log10(amount_precision)))
                adjusted_desired_base_amount = math.floor(desired_base_amount * (10 ** decimal_places)) / (10 ** decimal_places)
                if adjusted_desired_base_amount == 0 and desired_base_amount > 0:
                    adjusted_desired_base_amount = (10 ** -decimal_places)
            else:  # Integer precision (e.g., 1, 10)
                adjusted_desired_base_amount = math.floor(desired_base_amount / amount_precision) * amount_precision
                if adjusted_desired_base_amount == 0 and desired_base_amount > 0:
                    adjusted_desired_base_amount = amount_precision
        else:  # Fallback precision
            adjusted_desired_base_amount = math.floor(desired_base_amount * 100) / 100
            if adjusted_desired_base_amount == 0 and desired_base_amount > 0:
                adjusted_desired_base_amount = 0.01

        logger.info(f"Desired base amount from {target_usdc_amount:.2f} USDC: {desired_base_amount:.8f}, "
                    f"adjusted for precision: {adjusted_desired_base_amount:.8f} {symbol.split('/')[0]}")

        if adjusted_desired_base_amount == 0 and target_usdc_amount > 0: # target_usdc_amount is already > 0 here
            logger.warning(f"Requested USDC amount {target_usdc_amount:.2f} is too small to be represented with market precision for {symbol}. "
                           f"Falling back to minimum viable amount.")
            return min_viable_base_amount
        elif adjusted_desired_base_amount < min_viable_base_amount:
            logger.warning(
                f"Requested trade amount ({adjusted_desired_base_amount:.8f} {symbol.split('/')[0]}) "
                f"is less than the minimum viable amount ({min_viable_base_amount:.8f} {symbol.split('/')[0]}). "
                f"Using minimum viable amount instead."
            )
            return min_viable_base_amount
        else:
            return adjusted_desired_base_amount

    def _check_market_active(self, symbol: str) -> None:
        """Checks if the market for the given symbol is active."""
        if not self.order_manager.is_market_active(symbol):
            logger.error(f"Market {symbol} is not active")
            raise MarketNotActiveError(f"Market {symbol} is not active")
        logger.info(f"Market {symbol} is active.")

    def _get_market_info(self, symbol: str) -> dict[str, Any]:
        """Retrieves market information for a symbol, raising an error if not found."""
        market_info: dict[str, Any] = self.order_manager.get_market_info(symbol)
        if not market_info:
            raise ValueError(f"Market information for {symbol} not found.")
        logger.info(f"Successfully fetched market info for {symbol}.") 
        if market_info['limits']['cost']['min'] is not None:
            logger.info(f"Market info - Min cost: {market_info['limits']['cost']['min']}")
        if market_info['limits']['amount']['min'] is not None:
            logger.info(f"Market info - Min amount: {market_info['limits']['amount']['min']}")
        return market_info

    def _get_ticker_info(self, symbol: str) -> dict[str, Any]:
        """Fetches ticker information for the given symbol."""
        ticker: dict[str, Any] = self.order_manager.get_ticker_info(symbol)
        if not ticker:
            raise ValueError(f"Ticker for {symbol} not found.")
        logger.info(f"Successfully fetched ticker info for {symbol}.")
        return ticker

    def _adjust_to_precision(self, base_value: float, precision_value: float | int) -> float:
        """Adjusts a base_value upwards to the given precision_value."""
        if not (isinstance(precision_value, (int, float)) and precision_value > 0):
            logger.warning(f"Invalid precision_value ({precision_value}) for adjustment, defaulting to 2 decimal places.")
            return math.ceil(base_value * 100) / 100

        if precision_value < 1:  # Decimal precision (e.g., 0.01)
            decimal_places: int = abs(math.floor(math.log10(precision_value)))
            return math.ceil(base_value * (10 ** decimal_places)) / (10 ** decimal_places)
        else:  # Integer precision (e.g., 1, 10)
            return math.ceil(base_value / precision_value) * precision_value

    def _calculate_min_order_amount(self, symbol: str, price: float, market_info: dict[str, Any], leverage: float) -> float:
        """Calculates the minimum order amount based on market rules and a minimum order value."""
        min_cost: float = market_info['limits']['cost']['min']
        amount_precision: float = market_info['precision']['amount']
        
        # Ensure min_cost is a float, default to 0 if None (though it should usually be present)
        min_cost_value: float = float(min_cost) if min_cost is not None else 0.0

        if price <= 0: # Avoid division by zero or negative price
            logger.error(f"Invalid price ({price}) for min_amount calculation of {symbol}.")
            raise ValueError(f"Price must be positive for min_amount calculation. Got {price}")

        raw_min_amount: float = min_cost_value / price if min_cost_value > 0 else 0.0
        
        min_amount: float
        if isinstance(amount_precision, (int, float)) and amount_precision > 0:
            if amount_precision < 1:  # Decimal precision (e.g., 0.01)
                decimal_places: int = abs(math.floor(math.log10(amount_precision)))
                min_amount = math.ceil(raw_min_amount * (10 ** decimal_places)) / (10 ** decimal_places)
            else:  # Integer precision (e.g., 1, 10)
                min_amount = math.ceil(raw_min_amount / amount_precision) * amount_precision
        else: # Fallback if precision is not a positive number or is zero/None
            min_amount = math.ceil(raw_min_amount * 100) / 100 # Default to 2 decimal places

        min_amount_limit: float = market_info['limits']['amount']['min']
        if min_amount_limit is not None and min_amount < float(min_amount_limit):
            min_amount = float(min_amount_limit)
        
        # Ensure order value meets $10 minimum, then apply $11 buffer
        # Adjust target values by leverage
        if leverage <= 0:
            logger.error(f"Invalid leverage ({leverage}) for min_order_value calculation. Must be > 0.")
            raise ValueError(f"Leverage must be positive for min_order_value calculation. Got {leverage}")

        # Ensure order value meets the minimum buffer (e.g., $11, adjusted by leverage)
        # The $10/leverage check is implicitly covered by this stricter $11/leverage check.
        effective_min_order_value_buffer: float = 11.0 / leverage
        
        if min_amount * price < effective_min_order_value_buffer:
            required_amount_for_buffer: float = effective_min_order_value_buffer / price
            min_amount = self._adjust_to_precision(required_amount_for_buffer, amount_precision)

        # Final check against min_amount_limit if calculations pushed it below again
        if min_amount_limit is not None and min_amount < float(min_amount_limit):
            min_amount = float(min_amount_limit)

        final_order_value: float = min_amount * price
        logger.info(f"Calculated min_amount for {symbol}: {min_amount} units (raw from min_cost: {raw_min_amount:.8f})")
        logger.info(f"Final order value: ${final_order_value:.2f} (target notional: >= ${effective_min_order_value_buffer * leverage:.2f}, effective margin target: >= ${effective_min_order_value_buffer:.2f} with {leverage}x leverage)")
        if min_amount_limit:
            logger.info(f"Market min amount limit: {min_amount_limit}")
            
        return min_amount

    def _calculate_dynamic_slippage(self, ticker_info: dict, current_price: float) -> float:
        """Calculates dynamic slippage based on bid-ask spread."""
        bid: Literal[0] | float = float(ticker_info.get('bid', 0)) if ticker_info.get('bid') else 0
        ask: Literal[0] | float = float(ticker_info.get('ask', 0)) if ticker_info.get('ask') else 0
        
        dynamic_slippage: float
        if bid > 0 and ask > 0 and current_price > 0 and ask >= bid : # Ensure price > 0 and ask >= bid
            spread_percentage: float = ((ask - bid) / current_price) * 100
            # Set slippage to at least 2x the spread, with a minimum of 0.5% and maximum of 5%
            dynamic_slippage = max(0.005, min(0.05, spread_percentage * 2 / 100))
            logger.info(f"Market bid: {bid}, ask: {ask}. Spread: {spread_percentage:.2f}%, Dynamic slippage: {dynamic_slippage*100:.2f}%")
        else:
            dynamic_slippage = 0.01  # Default 1% slippage
            logger.info(f"No valid bid/ask data or zero price, using default slippage: {dynamic_slippage*100:.2f}%")
        return dynamic_slippage

    def _check_wallet_balance(self, required_usdc_amount: float) -> None:
        """Checks if there is sufficient USDC balance in the wallet."""
        balances = self.wallet_manager.get_balance()
        if not balances or 'USDC' not in balances or 'free' not in balances['USDC']: 
            msg: str = "Cannot fetch wallet balance for USDC or missing 'free' field."
            logger.error(msg)
            raise WalletBalanceError(msg)

        available_balance: float = float(balances['USDC']['free'])
        logger.info(f"Available USDC balance: {available_balance:.2f}")
        
        if available_balance < required_usdc_amount:
            msg: str = f"Insufficient balance: {available_balance:.2f} USDC, required: {required_usdc_amount:.2f} USDC"
            logger.warning(msg)
            raise WalletBalanceError(msg)
        logger.info("Wallet balance check passed.")

    def execute_trade(self, 
                        symbol: str,
                        side: str,
                        order_type: str = 'market',
                        target_usdc_amount: float | None = None,
                        take_profit_price: float | None = None,
                        stop_loss_price: float | None = None,
                        leverage: int = 2) -> dict[str, Any]:
        """
        Handles the checks and executes a trade for the given symbol.
        If take_profit_price or stop_loss_price are provided, it attempts to place them
        as separate orders after the main order is successfully placed.
        """
        logger.info(f"--- Attempting to execute {side} {order_type} order for {symbol} ---") 
        if stop_loss_price:
            logger.info(f"Planned Stop Loss Price: {stop_loss_price}")
        if take_profit_price:
            logger.info(f"Planned Take Profit Price: {take_profit_price}")
        
        main_order_result: dict[str, Any] = {}
        sl_order_result: dict[str, Any] = {}
        tp_order_result: dict[str, Any] = {}

        # Optional: Warning for symbol format, specific to Hyperliquid's common pattern
        if ':' not in symbol:
            logger.warning(f"Symbol {symbol} may not be a typical futures/perpetual pair for Hyperliquid (usually contains ':'). Proceeding.")
        
        try: 
            # 1. Market active check
            self._check_market_active(symbol) 

            # 2. Fetch ticker & determine current price
            ticker_info: dict[str, Any] = self._get_ticker_info(symbol)
            current_price: float = float(ticker_info['last']) if float(ticker_info['last']) else float(ticker_info['ask'])
            if not current_price or current_price <= 0:
                logger.error(f"Invalid current price ({current_price}) for {symbol} from ticker. Last: {ticker_info.get('last')}, Ask: {ticker_info.get('ask')}")
                raise TickerFetchError(f"Invalid or zero/negative current price ({current_price}) for {symbol} from ticker.") 
 
            # Validate TP/SL prices against current price and side
            if side == 'buy':
                if take_profit_price is not None and take_profit_price <= current_price:
                    raise ValueError(f"For a 'buy' order, Take Profit price ({take_profit_price}) must be greater than current price ({current_price}).")
                if stop_loss_price is not None and stop_loss_price >= current_price:
                    raise ValueError(f"For a 'buy' order, Stop Loss price ({stop_loss_price}) must be less than current price ({current_price}).")
            elif side == 'sell':
                if take_profit_price is not None and take_profit_price >= current_price:
                    raise ValueError(f"For a 'sell' order, Take Profit price ({take_profit_price}) must be less than current price ({current_price}).")
                if stop_loss_price is not None and stop_loss_price <= current_price:
                    raise ValueError(f"For a 'sell' order, Stop Loss price ({stop_loss_price}) must be greater than current price ({current_price}).")

            # 3. Calculate minimum viable base amount
            market_info: dict[str, Any] = self._get_market_info(symbol)

            # Pass leverage to _calculate_min_order_amount
            min_viable_base_amount: float = self._calculate_min_order_amount(symbol, current_price, market_info, leverage)
            final_base_amount_to_trade: float # final base amount to trade in units 

            if target_usdc_amount is not None and target_usdc_amount > 0:
                final_base_amount_to_trade = self._usdc_to_base_amount(
                    target_usdc_amount=target_usdc_amount, # target_usdc_amount is now guaranteed to be float > 0
                    current_price=current_price,
                    market_info=market_info,
                    min_viable_base_amount=min_viable_base_amount,
                    symbol=symbol
                )
            else:
                logger.info("No target USDC amount specified or it's invalid. Using calculated minimum viable trade amount.")
                final_base_amount_to_trade = min_viable_base_amount
            
            logger.info(f"Final base amount to trade for {symbol}: {final_base_amount_to_trade:.8f} units.")

            # 4. Calculate dynamic slippage
            dynamic_slippage: float = self._calculate_dynamic_slippage(ticker_info, current_price)

            # 5. Calculate estimated cost and check wallet balance
            estimated_cost_for_order: float = final_base_amount_to_trade * current_price * (1 + dynamic_slippage)
            # Calculate the actual margin required based on leverage
            # Ensure leverage is not zero to avoid DivisionByZeroError, though typically leverage is >= 1
            if leverage <= 0:
                logger.error(f"Invalid leverage value ({leverage}) for margin calculation. Must be greater than 0.")
                raise ValueError(f"Leverage must be greater than 0, got {leverage}")
            required_margin: float = estimated_cost_for_order / leverage
            
            logger.info(f"Estimated cost (notional value) with slippage for {final_base_amount_to_trade:.8f} units: {estimated_cost_for_order:.2f} USDC")
            logger.info(f"Required margin for this order (with {leverage}x leverage): {required_margin:.2f} USDC")
            self._check_wallet_balance(required_margin) # Pass required_margin instead of estimated_cost_for_order

            # 6. Set leverage for the symbol on the exchange
            logger.info(f"Attempting to set leverage for {symbol} to {int(leverage)}x before placing order.")
            self.order_manager.set_leverage_for_symbol(symbol, int(leverage)) # Assuming set_leverage_for_symbol handles if exchange doesn't support it
            
            # 7. Place main order
            logger.info("All pre-flight checks passed. Ready to place main order.")
            
            main_order_params: dict[str, Any] = {'slippage': dynamic_slippage, 'leverage': leverage}
            logger.info(f"Constructed main_order_params: {main_order_params}")

            main_order_result = self.order_manager.create_order(
                symbol=symbol,
                type=order_type,
                side=side,
                amount=final_base_amount_to_trade,
                price=current_price, # Pass current_price for market orders as well, as required by Hyperliquid
                params=main_order_params
            )
            self._log_order_summary("Main order placement", main_order_result)

            # If main order is successful, attempt to place TP/SL orders
            if main_order_result and main_order_result.get('id'):
                logger.info(f"Main order ID: {main_order_result.get('id')}. Proceeding with TP/SL if specified.")

                # Determine side for TP/SL orders (opposite of main order)
                opposite_side = 'sell' if side == 'buy' else 'buy'

                # Track if TP and SL orders were successfully placed
                tp_success = False
                sl_success = False

                # Place Stop Loss Order
                if stop_loss_price is not None:
                    try:
                        logger.info(f"Attempting to place Stop Loss order for {symbol} at {stop_loss_price}")
                        sl_params = {'reduceOnly': True}
                        # Based on GitHub issue, for Hyperliquid SL: amount=0, price=trigger_price 
                        sl_params['stopPrice'] = stop_loss_price

                        sl_order_result = self.order_manager.create_order(
                            symbol=symbol,
                            type='STOP_MARKET', # Or 'stop'. Could also be 'stop_limit' if a limit_price for SL is desired.
                            side=opposite_side,
                            amount=0, # Crucial for Hyperliquid SL according to GitHub issue
                            price=stop_loss_price, # This is the trigger price for STOP_MARKET
                            params=sl_params
                        )
                        self._log_order_summary("Stop Loss order placement", sl_order_result)
                        sl_success = True
                    except Exception as e_sl:
                        logger.error(f"Failed to place Stop Loss order for {symbol}: {e_sl}")
                        sl_order_result = None

                # Place Take Profit Order
                if take_profit_price is not None:
                    try:
                        logger.info(f"Attempting to place Take Profit order for {symbol} at {take_profit_price}")
                        tp_params = {'reduceOnly': True}
                        tp_order_result = self.order_manager.create_order(
                            symbol=symbol,
                            type='LIMIT', # Take profit is typically a limit order
                            side=opposite_side,
                            amount=final_base_amount_to_trade, # TP amount should match the main trade
                            price=take_profit_price,
                            params=tp_params
                        )
                        self._log_order_summary("Take Profit order placement", tp_order_result)
                        tp_success = True
                    except Exception as e_tp:
                        logger.error(f"Failed to place Take Profit order for {symbol}: {e_tp}")
                        tp_order_result = None

                # If both TP and SL were attempted but both failed, cancel the main order
                if (stop_loss_price is not None and take_profit_price is not None) and not (tp_success or sl_success):
                    logger.warning("Both TP and SL orders failed. Cancelling main order.")
                    try:
                        self.order_manager.cancel_order(main_order_result['id'], symbol)
                        logger.info(f"Successfully cancelled main order {main_order_result.get('id')} due to both TP and SL failures.")
                        raise DependentOrderError(f"Both Take Profit and Stop Loss orders failed for {symbol}. Main order has been cancelled.")
                    except Exception as e_cancel:
                        logger.error(f"Failed to cancel main order {main_order_result.get('id')}: {e_cancel}")
                        raise DependentOrderError(f"Both TP and SL orders failed, and main order cancellation also failed for {symbol}.") from e_cancel
                # If only one of TP or SL was attempted and failed, raise error
                elif (stop_loss_price is not None and not sl_success) or (take_profit_price is not None and not tp_success):
                    raise DependentOrderError(f"Failed to place {'Stop Loss' if not sl_success else 'Take Profit'} order for {symbol}. Main order remains active.")
            
            # Prepare final results object
            trade_execution_summary = {
                "main_order_id": main_order_result.get('id') if main_order_result else None,
                "stop_loss_order_id": sl_order_result.get('id') if sl_order_result else None,
                "take_profit_order_id": tp_order_result.get('id') if tp_order_result else None,
            }
            logger.info(f"Trade execution summary for {symbol}: {trade_execution_summary}")
            
            return {
                "main_order": main_order_result,
                "stop_loss_order": sl_order_result,
                "take_profit_order": tp_order_result
            }

        except (MarketNotActiveError, MarketInfoError, TickerFetchError, WalletBalanceError, ValueError, DependentOrderError) as e:  # Added DependentOrderError here
            logger.error(f"Trade execution aborted for {symbol}: {e}")
            raise 
        except Exception as e:
            logger.exception(f"Unexpected error occurred during trade execution for {symbol}")
            raise

# --- example execution used ---
# if __name__ == "__main__":
#     logger.info("=== Script Start: Hyperliquid Future Execution ===")
#     executor: FutureExecution = FutureExecution()
#     selected_trading_symbol: str = 'BTC/USDC:USDC'
#     trade_side_to_execute: str = 'buy' # 'buy' or 'sell' 
#     target_trade_value_usdc: float | None = 12 # Use a value that meets min requirements, e.g. $11 + buffer levrage money you want to use
    
#     # Set your desired percentages (e.g., 2% stop loss, 5% take profit)
#     tp_percent = 5.0  # 5% take profit
#     sl_percent = 2.0  # 2% stop loss
    
#     # Get the current price to calculate SL/TP
#     ticker = executor._get_ticker_info(selected_trading_symbol)
#     current_price = float(ticker['last'])
    
#     # Calculate SL and TP prices based on the trade side
#     if trade_side_to_execute.lower() == 'buy':
#         # For LONG: TP above entry, SL below entry
#         tp_price = current_price * (1 + tp_percent/100)
#         sl_price = current_price * (1 - sl_percent/100)
#     else:  # Short position
#         # For SHORT: TP below entry, SL above entry
#         tp_price = current_price * (1 - tp_percent/100)
#         sl_price = current_price * (1 + sl_percent/100)
    
#     try:
#         trade_results: dict[str, Any] = executor.execute_trade(
#             symbol=selected_trading_symbol, 
#             side=trade_side_to_execute,
#             target_usdc_amount=target_trade_value_usdc,
#             leverage=2,
#             stop_loss_price=sl_price,
#             take_profit_price=tp_price
#         )
#         # Using the new helper for final logging as well
#         executor._log_order_summary("Final Main order result", trade_results.get('main_order'))
#         if sl_price: # Only log SL if it was attempted
#             executor._log_order_summary("Final Stop Loss order result", trade_results.get('stop_loss_order'))
#         if tp_price: # Only log TP if it was attempted
#             executor._log_order_summary("Final Take Profit order result", trade_results.get('take_profit_order'))

#         if trade_results.get('main_order') and trade_results['main_order'].get('id'):
#             logger.info(f"Trade execution process completed for {selected_trading_symbol}.")
#         else: 
#             logger.warning(f"Main trade for {selected_trading_symbol} might have failed or no ID returned.")

#     except Exception as e: 
#         logger.error(f"MAIN: Trade execution failed for {selected_trading_symbol}. Reason: {e}")
    
#     logger.info("=== Script End ===")