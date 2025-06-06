import logging

logger = logging.getLogger("ccxt_hyperliquid")
logger.setLevel(logging.INFO)

# Clear existing handlers
if logger.hasHandlers():
    logger.handlers.clear()

formatter = logging.Formatter(
    "[%(asctime)s][%(levelname)s|%(filename)s:%(lineno)s] > %(message)s"
)
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)