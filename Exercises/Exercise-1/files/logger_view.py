"""Handles logging of file download and extraction."""
import logging

class LoggerView:
    """Logger class to handle info and error logging."""
    def __init__(self):
        """Initialize logger configuration."""
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

    def log_info(self, message):
        """Log information messages."""
        self.logger.info(message)

    def log_error(self, message):
        """Log error messages."""
        self.logger.error(message)
