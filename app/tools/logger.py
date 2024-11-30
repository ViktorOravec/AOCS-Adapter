import logging

class CustomLogger:
    def __init__(self, name: str, log_level: str = 'INFO', console_output: bool = True):
        """
        Initializes the custom logger.

        :param name: The name of the logger (usually the module name)
        :param log_level: The minimum level of logging (default is 'INFO')
        :param console_output: Whether or not to also log to the console (default is True)
        """
        # Set up the logger
        self.logger = logging.getLogger(name)
        self.logger.setLevel(self._get_log_level(log_level))

        # Create a console handler if console_output is True
        if console_output:
            console_handler = logging.StreamHandler()
            console_handler.setLevel(self._get_log_level(log_level))

            # Create a formatter and add it to the handler
            log_format = '%(asctime)s [%(name)s] %(levelname)-8s - %(message)s'
            formatter = logging.Formatter(log_format)
            console_handler.setFormatter(formatter)

            # Add the console handler to the logger
            self.logger.addHandler(console_handler)

    def _get_log_level(self, log_level: str):
        """Helper method to get the logging level from a string"""
        levels = {
            'CRITICAL': logging.CRITICAL,
            'ERROR': logging.ERROR,
            'WARNING': logging.WARNING,
            'INFO': logging.INFO,
            'DEBUG': logging.DEBUG,
            'NOTSET': logging.NOTSET
        }
        return levels.get(log_level.upper(), logging.INFO)

    def log(self, message: str, level: str = 'INFO'):
        """
        Log a message at a specific level.

        :param message: The message to log
        :param level: The log level (default is 'INFO')
        """
        log_method = getattr(self.logger, level.lower(), self.logger.info)
        log_method(message)

    def info(self, message: str):
        """Log an info message"""
        self.log(message, 'INFO')

    def debug(self, message: str):
        """Log a debug message"""
        self.log(message, 'DEBUG')

    def warning(self, message: str):
        """Log a warning message"""
        self.log(message, 'WARNING')

    def error(self, message: str):
        """Log an error message"""
        self.log(message, 'ERROR')

    def critical(self, message: str):
        """Log a critical message"""
        self.log(message, 'CRITICAL')


# Usage example
if __name__ == '__main__':
    logger = CustomLogger(name='MyAppLogger', log_level='DEBUG')

    logger.debug("This is a debug message.")
    logger.info("This is an info message.")
    logger.warning("This is a warning message.")
    logger.error("This is an error message.")
    logger.critical("This is a critical message.")
