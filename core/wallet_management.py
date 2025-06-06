from ..ccxt_base import CcxtBase
from typing import Optional, Dict, Any, List
from ..log.logger import logger

class CcxtWalletManagement(CcxtBase):
    """
    Manages wallet operations for Hyperliquid using the inherited CCXT exchange instance.
    All actions related to wallet management are logged.
    """

    def __init__(self) -> None:
        super().__init__()
        if not self.exchange:
            logger.warning("Hyperliquid exchange not initialized. Wallet operations may fail.")

    def get_balance(self, wallet_type: str = 'margin') -> Optional[Dict[str, Any]]:
        """
        Fetches the account balance for the specified wallet type.
        
        Args:
            wallet_type: Type of wallet to fetch balance for (e.g., 'spot', 'margin', 'funding')
            
        Returns:
            Optional[Dict[str, Any]]: Dictionary containing balance information, or None if an error occurs
        """
        if not self.exchange:
            logger.error("Exchange not initialized. Cannot fetch balance.")
            return None
        try:
            # Forward wallet_type to exchange params if supported
            params: Dict[str, str] = {}
            if wallet_type:
                params['type'] = wallet_type  # 'spot', 'margin', 'funding', etc. (depends on exchange)


            balance: Dict[str, Any] = self.exchange.fetch_balance(params)
            logger.info(f"Balance fetched for wallet_type={wallet_type}")
            return balance
        except Exception as e:
            self._handle_operation_error("fetch_balance", e)
            return None


    def withdraw(
        self,
        asset: str,
        amount: float,
        address: str,
        tag: Optional[str] = None,
        params: Optional[Dict[str, Any]] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Withdraws funds from the exchange to the specified address.
        
        Args:
            asset: The asset to withdraw (e.g., 'BTC', 'ETH')
            amount: The amount to withdraw
            address: The destination address
            tag: Optional memo/tag for the withdrawal
            params: Additional parameters for the withdrawal
            
        Returns:
            Optional[Dict[str, Any]]: Withdrawal details if successful, None otherwise
        """
        if not self.exchange:
            logger.error("Exchange not initialized. Cannot withdraw.")
            return None
        try:
            result: Dict[str, Any] = self.exchange.withdraw(asset, amount, address, tag, params or {})
            logger.info(f"Withdraw successful for {amount} {asset} to {address}")
            return result
        except Exception as e:
            self._handle_operation_error("withdraw", e)
            return None

    def get_deposit_address(self, asset: str) -> Optional[Dict[str, Any]]:
        """
        Fetches the deposit address for the specified asset.
        
        Args:
            asset: The asset to get the deposit address for (e.g., 'BTC', 'ETH')
            
        Returns:
            Optional[Dict[str, Any]]: Dictionary containing the deposit address and related info,
                                     or None if an error occurs
        """
        if not self.exchange:
            logger.error("Exchange not initialized. Cannot get deposit address.")
            return None
        try:
            address: Dict[str, Any] = self.exchange.fetch_deposit_address(asset)
            logger.info(f"Deposit address fetched for {asset}")
            return address
        except Exception as e:
            self._handle_operation_error("fetch_deposit_address", e)
            return None

    def transfer(
        self,
        asset: str,
        amount: float,
        from_account: str,
        to_account: str,
        params: Optional[Dict[str, Any]] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Transfers funds between accounts on the exchange.
        
        Args:
            asset: The asset to transfer (e.g., 'USDT', 'BTC')
            amount: The amount to transfer
            from_account: Source account identifier
            to_account: Destination account identifier
            params: Additional parameters for the transfer
            
        Returns:
            Optional[Dict[str, Any]]: Transfer details if successful, None otherwise
        """
        if not self.exchange:
            logger.error("Exchange not initialized. Cannot transfer.")
            return None
        try:
            result: Dict[str, Any] = self.exchange.transfer(
                asset, amount, from_account, to_account, params or {}
            )
            logger.info(f"Transfer of {amount} {asset} from {from_account} to {to_account} successful")
            return result
        except Exception as e:
            self._handle_operation_error("transfer", e)
            return None

    def get_transaction_history(
        self,
        asset: Optional[str] = None,
        since: Optional[int] = None,
        limit: Optional[int] = None,
        params: Optional[Dict[str, Any]] = None
    ) -> Optional[List[Dict[str, Any]]]:
        """
        Fetches the transaction history for the account.
        
        Args:
            asset: Filter transactions by asset (optional)
            since: Timestamp in milliseconds for the start time (optional)
            limit: Maximum number of transactions to return (optional)
            params: Additional parameters for the API request
            
        Returns:
            Optional[List[Dict[str, Any]]]: List of transactions if successful, None otherwise
        """
        if not self.exchange:
            logger.error("Exchange not initialized. Cannot fetch transaction history.")
            return None
        try:
            history: List[Dict[str, Any]] = self.exchange.fetch_transactions(
                asset, since, limit, params or {}
            )
            logger.info(f"Fetched {len(history)} transactions")
            return history
        except Exception as e:
            self._handle_operation_error("fetch_transactions", e)
            return None