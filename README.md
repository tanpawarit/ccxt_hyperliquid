# ccxt_hyperliquid

A Python package for Hyperliquid exchange integration using a CCXT-like interface.

## Project Structure

```
ccxt_hyperliquid/
├── core/                         # Main business logic and trading operations
│   ├── __init__.py               # Core package initializer
│   ├── order_management.py       # Order creation, management, and execution logic
│   ├── wallet_management.py      # Wallet and balance management
│   ├── data_management.py        # Market data fetching and utilities
│   ├── portfolio_management.py   # Portfolio and position management
│   └── executor.py               # Trade execution and order orchestration
├── adapter/                      # Integrations and adapters for external signals/sources
│   ├── __init__.py               # Adapter package initializer
│   └── adapter.py                # Signal adapter (e.g., for Twitter or other sources)
├── log/                          # Logging utilities
│   └── logger.py                 # Centralized logger configuration
├── ccxt_base.py                  # Base class for CCXT integration (singleton pattern)
├── main.py                       # Application entry point
```


## Features
- Unified interface for trading on Hyperliquid exchange
- Modular architecture for easy extension and maintenance
- Signal integration and portfolio management
- Logging and error handling best practices

## Installation

1. Clone this repository:
   ```sh
   git clone <your-repo-url>
   cd ccxt_hyperliquid
   ```

2. Install dependencies using [uv](https://github.com/astral-sh/uv):
   ```sh
   uv pip install -r pyproject.toml
   ```
   Or use pip:
   ```sh
   pip install -r requirements.txt  # if you generate one from pyproject.toml
   ```

## Usage

- Configure your Hyperliquid API credentials (e.g., via environment variables or a config file as expected by `get_config`).
- Run the main entry point:
  ```sh
  python main.py
  ```
- The system will manage portfolio, signals, and execute trades as defined in the business logic.

## Development

- Follow PEP8 and use type hints throughout the codebase.
- Project dependencies are managed with `pyproject.toml` and `uv`.
- For testing and extension, add new modules to `core/` or `adapter/` as appropriate.
 
## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.