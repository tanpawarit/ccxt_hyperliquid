from ..ccxt_base import CcxtBase
from typing import Optional, List, Dict, Any
import ccxt # type: ignore
from ..log.logger import logger

class CcxtOrderManagement(CcxtBase):
    """
    Manages trading operations (placing, canceling, fetching orders)
    specifically for Hyperliquid using the inherited CCXT exchange instance.
    All messages related to order management operations are logged.
    """

    def __init__(self) -> None:
        """
        Initializes the CcxtOrderManagement by calling the parent CcxtBase
        constructor, which ensures the CCXT Hyperliquid exchange is initialized.
        """
        super().__init__()
        # Ensure the exchange is available before proceeding with order operations
        if not self.exchange:
            logger.warning("Hyperliquid exchange not initialized. Order operations may fail.")

    def create_order(
        self,
        symbol: str,
        type: str,  # e.g., 'limit', 'market'
        side: str,  # e.g., 'buy', 'sell'
        amount: float,
        price: Optional[float] = None,
        params: Optional[Dict[str, Any]] = None # Extra parameters for specific order types
    ) -> Dict[str, Any]:  
        """
        Places a new order on Hyperliquid.

        Args:
            symbol (str): The trading pair (e.g., 'ETH/USD').
            type (str): The type of order ('limit', 'market').
            side (str): The order side ('buy', 'sell').
            amount (float): The amount of base currency to trade.
            price (Optional[float]): The price for 'limit' orders. Required for 'limit' orders.
            params (Optional[Dict[str, Any]]): Additional exchange-specific parameters.

        Returns:
            Dict[str, Any]: A dictionary containing the order details.

        Raises:
            RuntimeError: If the exchange is not initialized.
            ValueError: If required parameters are missing or invalid (e.g., price for limit order, non-positive amount).
            ccxt.NetworkError: For network-related issues.
            ccxt.ExchangeError: For exchange-specific errors (e.g., insufficient funds, invalid order).
            Exception: For any other unexpected errors during order creation.
        """
        if not self.exchange:
            logger.error("Exchange not initialized. Cannot create order.")
            raise RuntimeError("Exchange not initialized. Cannot create order.")
        
        if type == 'limit' and price is None:
            logger.error("Price is required for limit orders.")
            raise ValueError("Price is required for limit orders.")
        
        # Allow amount = 0 for STOP_MARKET orders (Hyperliquid specific for SL)
        if not (type.upper() == 'STOP_MARKET' and amount == 0):
            if amount <= 0:
                logger.error(f"Order amount must be positive, or 0 for STOP_MARKET. Got: {amount} for type: {type}")
                raise ValueError(f"Order amount must be positive, or 0 for STOP_MARKET. Got: {amount} for type: {type}")

        try:
            order: Dict[str, Any] = self.exchange.create_order(symbol, type, side, amount, price, params)
            logger.info(f"Order created successfully: {order}")
            return order
        except Exception as e: 
            self._handle_operation_error(f"creating {side} {type} order for {symbol}", e) 
            raise 
            
    def set_leverage_for_symbol(self, symbol: str, leverage: int, params: Optional[Dict[str, Any]] = None) -> None:
        """
        Sets the leverage for a specific trading symbol.

        Args:
            symbol (str): The trading pair (e.g., 'BTC/USDC:USDC').
            leverage (int): The desired leverage value.
            params (Optional[Dict[str, Any]]): Additional exchange-specific parameters for setting leverage.

        Raises:
            RuntimeError: If the exchange is not initialized.
            NotSupportedError: If the exchange does not support setting leverage via CCXT.
            ccxt.NetworkError: For network-related issues.
            ccxt.ExchangeError: For exchange-specific errors (e.g., invalid leverage, symbol not found).
            Exception: For any other unexpected errors.
        """
        if not self.exchange:
            logger.error("Exchange not initialized. Cannot set leverage.")
            raise RuntimeError("Exchange not initialized. Cannot set leverage.")
        
        if not self.exchange.has.get('setLeverage'):
            msg: str = f"Exchange {self.exchange.id} does not support setLeverage via CCXT."
            # Create an instance of the error to pass to the handler
            error_instance: ccxt.ExchangeError = ccxt.ExchangeError(msg) 
            self._handle_operation_error(operation=f"checking setLeverage support for {self.exchange.id}", error=error_instance)
            raise error_instance # Re-raise the specific error instance

        try:
            logger.info(f"Attempting to set leverage for {symbol} to {leverage}x with params: {params}") 
            self.exchange.set_leverage(leverage, symbol, params) 
            logger.info(f"Successfully set leverage for {symbol} to {leverage}x")
        except Exception as e: # Catch any exception during the set_leverage call
            self._handle_operation_error(operation=f"setting leverage for {symbol} to {leverage}x", error=e)
            raise  
            
    def cancel_order(self, order_id: str, symbol: str) -> Dict[str, Any]: 
        """
        Cancels an open order on Hyperliquid.
        """
        if not self.exchange:
            logger.error("Exchange not initialized. Cannot cancel order.")
            raise RuntimeError("Exchange not initialized. Cannot cancel order.")

        try:
            cancellation: Dict[str, Any] = self.exchange.cancel_order(order_id, symbol)
            logger.info(f"Order {order_id} cancelled successfully: {cancellation}")
            return cancellation
        except Exception as e:
            self._handle_operation_error(f"cancelling order {order_id} for {symbol}", e)
            raise

    def fetch_order(self, order_id: str, symbol: str) -> Dict[str, Any]: 
        """
        Fetches details of a specific order by its ID.
        """
        if not self.exchange:
            logger.error("Exchange not initialized. Cannot fetch order.")
            raise RuntimeError("Exchange not initialized. Cannot fetch order.")

        try:
            order: Dict[str, Any] = self.exchange.fetch_order(order_id, symbol)
            return order
        except Exception as e:
            self._handle_operation_error(f"fetching order {order_id} for {symbol}", e)
            raise

    def fetch_open_orders(self, symbol: Optional[str] = None) -> List[Dict[str, Any]]: 
        """
        Fetches all currently open orders.
        """
        if not self.exchange:
            logger.error("Exchange not initialized. Cannot fetch open orders.")
            raise RuntimeError("Exchange not initialized. Cannot fetch open orders.")

        try:
            orders: List[Dict[str, Any]] = self.exchange.fetch_open_orders(symbol)
            logger.info(f"Fetched {len(orders)} open orders for {symbol or 'all symbols'}.")
            return orders
        except Exception as e:
            self._handle_operation_error(f"fetching open orders for {symbol or 'all symbols'}", e)
            raise

    def fetch_closed_orders(self, symbol: Optional[str] = None) -> List[Dict[str, Any]]: 
        """
        Fetches all closed (filled or canceled) orders.
        """
        if not self.exchange:
            logger.error("Exchange not initialized. Cannot fetch closed orders.")
            raise RuntimeError("Exchange not initialized. Cannot fetch closed orders.")

        try:
            orders: List[Dict[str, Any]] = self.exchange.fetch_closed_orders(symbol)
            logger.info(f"Fetched {len(orders)} closed orders for {symbol or 'all symbols'}.")
            return orders
        except Exception as e:
            self._handle_operation_error(f"fetching closed orders for {symbol or 'all symbols'}", e)
            raise
            
    def fetch_balance(self) -> Dict[str, Any]: # Changed return type
        """
        Fetches the account balance for all assets on Hyperliquid.
        """
        if not self.exchange:
            logger.error("Exchange not initialized. Cannot fetch balance.")
            raise RuntimeError("Exchange not initialized. Cannot fetch balance.")
        
        try:
            balance: Dict[str, Any] = self.exchange.fetch_balance()
            logger.info("Balance fetched successfully.")
            return balance
        except Exception as e:
            self._handle_operation_error("fetching balance", e)
            raise

    def fetch_positions(self) -> List[Dict[str, Any]]: 
        """
        Fetches all open positions on Hyperliquid.
        """
        if not self.exchange:
            logger.error("Exchange not initialized. Cannot fetch positions.")
            raise RuntimeError("Exchange not initialized. Cannot fetch positions.")

        try:
            positions: List[Dict[str, Any]] = self.exchange.fetch_positions()
            logger.info(f"Fetched {len(positions)} positions.")
            return positions
        except Exception as e:
            self._handle_operation_error(f"fetching positions", e) 
            raise

    def close_position_by_symbol(self, symbol_to_close: str) -> None:
        """
        Closes a specific open futures position by its symbol.
        Submits a market order in the opposite direction.
        For Hyperliquid, price is required even for market orders.
        """
        if not self.exchange:
            logger.error("Exchange not initialized. Cannot close position.")
            raise RuntimeError("Exchange not initialized. Cannot close position.")

        if ':' not in symbol_to_close:
            logger.warning(
                f"Symbol '{symbol_to_close}' does not appear to be a futures contract (e.g., 'ETH/USDC:USDC'). "
                "This function is intended for futures positions."
            )
            # Optionally, you might want to raise an error or simply return if the symbol format is incorrect.
            return

        try:
            positions: List[Dict[str, Any]] = self.fetch_positions()
            if not positions:
                logger.info(f"No open positions found. Cannot close '{symbol_to_close}'.")
                return

            target_position: Optional[Dict[str, Any]] = None
            for pos in positions:
                current_symbol = pos.get('symbol', '')
                # Ensure it's a futures position and matches the target symbol
                if ':' in current_symbol and current_symbol == symbol_to_close:
                    target_position = pos
                    break
            
            if not target_position:
                logger.info(f"No open futures position found for symbol '{symbol_to_close}'.")
                return

            symbol: str = target_position.get('symbol', '') # Should be symbol_to_close
            # Consolidate fetching amount from various possible keys
            amt_str = target_position.get('contracts') or target_position.get('positionAmt') or target_position.get('amount')
            amt: float = float(amt_str) if amt_str is not None else 0.0
            
            side: str = target_position.get('side', '').lower()

            if amt == 0:
                logger.info(f"Position for '{symbol}' has zero amount. No action needed.")
                return
            
            # Determine the side of the closing order
            close_side: str
            if side in ['long', 'buy'] or (side == '' and amt > 0): # Position is long
                close_side = 'sell'
            elif side in ['short', 'sell'] or (side == '' and amt < 0): # Position is short
                close_side = 'buy'
            else:
                logger.error(f"Could not determine position side for '{symbol}'. Amount: {amt}, Side: '{side}'. Cannot close.")
                return

            close_amt: float = abs(amt)
            
            try:
                # Fetch current price for the symbol (required for Hyperliquid market orders)
                ticker: dict[str, Any] = self.exchange.fetch_ticker(symbol)
                price_last = ticker.get('last')
                price_ask = ticker.get('ask')
                
                price: Optional[float] = None
                if price_last is not None:
                    price = float(price_last)
                elif price_ask is not None: # Use ask if last is not available
                    price = float(price_ask)

                if price is None or price <= 0:
                    logger.error(f"Cannot get a valid positive price for '{symbol}'. Last: {price_last}, Ask: {price_ask}. Skipping close.")
                    return 
                
                # Prepare params for closing position
                params: Dict[str, Any] = {
                    'reduceOnly': True,
                    'slippage': 0.01  # Example: 1% slippage, adjust as needed
                }
                
                logger.info(f"Attempting to close position for '{symbol}': side='{close_side}', amount={close_amt}, price={price}, params={params}")
                order: dict[str, Any] = self.create_order(symbol, 'market', close_side, close_amt, price=price, params=params)
                logger.info(f"Successfully submitted market order to close position for '{symbol}'. Order ID: {order.get('id', 'N/A')}")
                
            except ccxt.NetworkError as e:
                logger.error(f"Network error while preparing or executing closing order for '{symbol}': {e}")
                self._handle_operation_error(f"closing order network for {symbol}", e)
                raise 
            except ccxt.ExchangeError as e:
                logger.error(f"Exchange error while preparing or executing closing order for '{symbol}': {e}")
                self._handle_operation_error(f"closing order exchange for {symbol}", e)
                raise
            except Exception as e:
                logger.error(f"Unexpected error while preparing or executing closing order for '{symbol}': {e}")
                self._handle_operation_error(f"closing order unexpected for {symbol}", e)
                raise

        except ccxt.NetworkError as e:
            logger.error(f"Network error while fetching positions for '{symbol_to_close}': {e}")
            self._handle_operation_error(f"fetching positions network for {symbol_to_close}", e)
            raise
        except ccxt.ExchangeError as e:
            logger.error(f"Exchange error while fetching positions for '{symbol_to_close}': {e}")
            self._handle_operation_error(f"fetching positions exchange for {symbol_to_close}", e)
            raise
        except Exception as e:
            logger.error(f"An unexpected error occurred while trying to close position for '{symbol_to_close}': {e}")
            self._handle_operation_error(f"closing position unexpected for {symbol_to_close}", e)
            raise

    def close_all_positions(self) -> None:
        """
        Closes all open futures positions by submitting market orders in the opposite direction.
        For Hyperliquid, price is required even for market orders.
        """
        if not self.exchange:
            logger.error("Exchange not initialized. Cannot close positions.")
            raise RuntimeError("Exchange not initialized. Cannot close positions.")

        try:
            positions: List[Dict[str, Any]] = self.fetch_positions()
            if not positions:
                logger.info("No open positions to close.")
                return
            
            futures_positions: list[Dict[str, Any]] = []
            for pos in positions:
                symbol = pos.get('symbol', '')
                # Filter only futures/perpetual positions (containing ':')
                if ':' in symbol:
                    futures_positions.append(pos)
            
            if not futures_positions:
                logger.info("No futures positions to close.")
                return
            
            logger.info(f"Found {len(futures_positions)} futures positions to close.")
            
            for pos in futures_positions:
                symbol: str = pos.get('symbol', '')
                amt: float = float(pos.get('contracts') or pos.get('positionAmt') or pos.get('amount') or 0) 
                side: str = pos.get('side', '').lower()
                
                if amt == 0 or not symbol:
                    logger.info(f"Skipping position with symbol={symbol}, amount={amt}")
                    continue
                
                # Determine close side based on position side or amount
                if side in ['long', 'buy'] or (side == '' and amt > 0):
                    close_side = 'sell'
                else:
                    close_side = 'buy'
                    
                close_amt: float = abs(amt)
                
                try:
                    # Fetch current price for the symbol (required for Hyperliquid market orders)
                    ticker: dict[str, Any] = self.exchange.fetch_ticker(symbol)
                    price: float = float(ticker['last']) if ticker.get('last') else float(ticker.get('ask', 0))
                    
                    if price <= 0:
                        logger.error(f"Cannot get valid price for {symbol}, skipping close.")
                        continue
                    
                    # Prepare params for closing position
                    params: Dict[str, Any] = {
                        'reduceOnly': True,
                        'slippage': 0.01  # 1% slippage
                    }
                    
                    logger.info(f"Closing futures position: {symbol}, size={close_amt}, side={close_side}, price={price}")
                    order: dict[str, Any] = self.create_order(symbol, 'market', close_side, close_amt, price=price, params=params)
                    logger.info(f"Position closed successfully: {order.get('id', 'N/A')}")
                    
                except Exception as e:
                    logger.error(f"Failed to close position for {symbol}: {e}")
                    
            logger.info("Finished closing all futures positions.")
            
        except Exception as e:
            logger.error(f"Failed to close all positions: {e}")
            raise
 
    def close_all_orders(self) -> None:
            """
            Cancels all currently open orders for all symbols on the exchange.
            """
            if not self.exchange:
                logger.error("Exchange not initialized. Cannot cancel orders.")
                raise RuntimeError("Exchange not initialized. Cannot cancel orders.")
            try:
                open_orders = self.fetch_open_orders()
                if not open_orders:
                    logger.info("No open orders to cancel.")
                    return
                for order in open_orders:
                    order_id = order.get('id') or order.get('order_id')
                    symbol = order.get('symbol')
                    if not order_id or not symbol:
                        logger.warning(f"Skipping order with missing id or symbol: {order}")
                        continue
                    try:
                        self.cancel_order(order_id, symbol)
                    except Exception as e:
                        logger.error(f"Failed to cancel order {order_id} for {symbol}: {e}")
                logger.info("Finished cancelling all open orders.")
            except Exception as e:
                logger.error(f"Failed to cancel all open orders: {e}")
                raise