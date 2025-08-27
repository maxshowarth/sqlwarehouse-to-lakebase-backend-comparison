import os
from loguru import logger
from app.config import get_config

class AppLogger:
    """Global logger configuration for the application.

    Sets the log level from get_config().log_level.
    """
    def __init__(self) -> None:
        log_level = get_config().log_level.upper()
        logger.remove()
        logger.add(
            sink=lambda msg: print(msg, end=""),
            level=log_level,
            format="<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>"
        )
        self.logger = logger

    def get_logger(self, name: str = None):
        """Get the configured logger instance.

        Args:
            name (str, optional): Name for the logger context. Defaults to None.
        Returns:
            loguru.Logger: The configured logger instance.
        """
        if name:
            return self.logger.bind(name=name)
        return self.logger

def get_logger(name: str = None):
    """Get a new application logger using the latest config."""
    return AppLogger().get_logger(name)