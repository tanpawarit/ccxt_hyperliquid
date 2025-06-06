import ccxt
from typing import Dict, Any, Optional
from .log.logger import logger
from config import get_config

class CcxtBase:
    """
    Base class for CCXT integration with Hyperliquid, implementing the Singleton pattern.
    This ensures that only a single instance of the Hyperliquid exchange connection
    and its market data is created and reused throughout the application.
    """
    _instance: Optional['CcxtBase'] = None
    _exchange: Optional[ccxt.Exchange] = None
    _markets: Optional[Dict[str, Any]] = None

    def __init__(self) -> None:
        # Explicitly declare these attributes to avoid implicit definition
        self.wallet_address: Optional[str] = None
        self.private_key: Optional[str] = None

    def __new__(cls, *args, **kwargs) -> 'CcxtBase':
        """
        Creates and returns the singleton instance of CcxtBase.
        Initializes the exchange connection if it's the first time.
        """
        if not cls._instance:
            cls._instance = super(CcxtBase, cls).__new__(cls, *args, **kwargs)
            cls._instance._initialize()
        return cls._instance

    def _initialize(self) -> None:
        """
        Initializes the CCXT Hyperliquid exchange instance and loads markets.
        This method is designed to be called only once during the singleton's creation.
        Handles API key validation and initial connection errors.
        """
        # If exchange is already initialized, skip
        if CcxtBase._exchange is not None:
            return

        # Retrieve API credentials from environment variables
        self.wallet_address = get_config(["hyperliquid", "wallet_address"])
        self.private_key = get_config(["hyperliquid", "private_key"])
        
        # Validate that API credentials are provided
        if not self.wallet_address or not self.private_key:
            error_msg = "Hyperliquid API credentials (HYPERLIQUID_WALLET_ADDRESS, HYPERLIQUID_PRIVATE_KEY) are missing from environment variables."
            logger.error(f"Error: {error_msg}")
            # Reset internal states to indicate failed initialization
            CcxtBase._exchange = None 
            CcxtBase._markets = None
            raise ccxt.AuthenticationError(error_msg) # Propagate a clear authentication error

        try:
            # Initialize the Hyperliquid exchange instance with provided credentials
            CcxtBase._exchange = ccxt.hyperliquid({
                'walletAddress': self.wallet_address,
                'privateKey': self.private_key, 
                'enableRateLimit': True,  # Always enable CCXT's built-in rate limiter for safe API usage
            })
            
            # Load all available markets from the exchange
            if CcxtBase._markets is None:
                CcxtBase._markets = CcxtBase._exchange.load_markets()
                logger.info("Hyperliquid API Connected: Markets loaded successfully.")
                
        except ccxt.NetworkError as e:
            self._handle_initialization_error("network connectivity", e)
        except ccxt.ExchangeError as e:
            self._handle_initialization_error("exchange specific", e)
        except ccxt.AuthenticationError as e:
            self._handle_initialization_error("authentication", e)
        except ccxt.BaseError as e:
            self._handle_initialization_error("CCXT library", e)
        except Exception as e:
            self._handle_initialization_error("unexpected", e)
        finally:
            # Ensure _exchange and _markets are set to None if initialization failed
            if CcxtBase._markets is None: # If markets failed to load, assume exchange is also not fully ready
                CcxtBase._exchange = None

    def _handle_initialization_error(self, error_type: str, error: Exception) -> None:
        """
        A dedicated error handler for issues encountered during the initial setup (_initialize method).
        It prints a descriptive error message and resets the internal exchange/markets state.
        """
        logger.error(f"Initialization error ({error_type}): {error}")
        CcxtBase._exchange = None
        CcxtBase._markets = None

    def _handle_operation_error(self, operation: str, error: Exception) -> None:
        """
        A centralized error handler for issues encountered during subsequent API operations.
        Provides specific messages based on the type of CCXT error.
        """
        if isinstance(error, ccxt.NetworkError):
            logger.error(f"Network error during {operation}: {error}")
        elif isinstance(error, ccxt.ExchangeError):
            logger.error(f"Exchange error during {operation}: {error}")
        elif isinstance(error, ccxt.AuthenticationError):
            logger.error(f"Authentication error during {operation}: {error}. Please verify your API credentials.")
        elif isinstance(error, ccxt.BaseError):
            logger.error(f"CCXT error during {operation}: {error}")
        else:
            logger.error(f"Unexpected error during {operation}: {error}")

    @property
    def exchange(self) -> Optional[ccxt.Exchange]:
        """
        Provides direct access to the initialized CCXT exchange object.
        Returns None if the exchange failed to initialize.
        """
        return self._exchange

    @property
    def markets(self) -> Optional[Dict[str, Any]]:
        """
        Provides direct access to the loaded market data dictionary.
        Returns None if market data failed to load.
        """
        return self._markets
 
    def get_market_info(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        Retrieves detailed ticker information for a given trading symbol.
        
        Args:
            symbol (str): The trading pair symbol (e.g., "ETH/USD").
            
        Returns:
            Optional[Dict[str, Any]]: A dictionary containing market details (e.g., precision, limits),
                                      or None if the market is not found or an error occurs.
        """
        if not self.markets:
            logger.error("Error: Markets not loaded. Cannot retrieve market information.")
            return None
        try: 
            return self.markets.get(symbol)
        except Exception as e:
            self._handle_operation_error(f"retrieving market info for {symbol}", e)
            return None

    def get_ticker_info(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        Fetches the current ticker information (e.g., last price, bid, ask) for a specific trading symbol.
        
        Returns:
            Optional[Dict[str, Any]]: A dictionary containing ticker data,
                                      or None if an error occurs during the fetch operation.
        """
        if not self.exchange:
            logger.error("Error: Exchange not initialized. Cannot fetch ticker.")
            return None
        try:
            return self.exchange.fetch_ticker(symbol)
        except Exception as e:
            self._handle_operation_error(f"fetching ticker for {symbol}", e)
            return None

    def is_market_active(self, symbol: str) -> bool:
        """
        Checks if a specific trading market is currently active and available for operations.
        
        Args:
            symbol (str): The trading pair symbol (e.g., "ETH/USD").
            
        Returns:
            bool: True if the market is active, False otherwise (e.g., if market is not found or inactive).
        """
        # get_ticker_info handles its own errors, so we just check its return value.
        market_info: dict[str, Any] | None = self.get_market_info(symbol)
        return market_info is not None and market_info.get('active', False)