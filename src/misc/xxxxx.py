"""""import logging
import os

# Ensure log directory exists
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

# Set up a single logger for all modules
logger = logging.getLogger("app")
logger.setLevel(logging.DEBUG)


# File handler (logs everything in one file)
file_handler = logging.FileHandler(os.path.join(LOG_DIR, "app.log"))
file_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(name)s - %(message)s")
file_handler.setFormatter(file_formatter)

# Console handler (only shows warnings and errors)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.WARNING)  # Only warnings & errors
console_formatter = logging.Formatter("CONSOLE: %(levelname)s - %(name)s - %(message)s")
console_handler.setFormatter(console_formatter)

if not logger.hasHandlers():
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
# Function to get module-specific loggers
def get_logger(module_name):
    return logging.getLogger(module_name)




#madule 1:
from logger_setup import setup_logger

logger = setup_logger("module_a", "module_a.log")

def process_data():
    logger.info("Processing data in module A...")
    logger.warning("This is a warning from module A!")

#main:
import module_a
import module_b
from logger_setup import setup_logger

logger = setup_logger("main", "main.log")

if __name__ == "__main__":
    logger.info("Starting the application...")
    module_a.process_data()
    module_b.fetch_data()
    logger.info("Application finished.")



import logging
import os

# Ensure log directory exists
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

def setup_logger(name, log_file, level=logging.DEBUG):
    """Creates a logger with a specific file."""
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # File handler (each module gets a unique file)
    file_handler = logging.FileHandler(os.path.join(LOG_DIR, log_file))
    file_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(name)s - %(message)s")
    file_handler.setFormatter(file_formatter)

    # Console handler (optional, logs to the terminal)
    console_handler = logging.StreamHandler()
    console_formatter = logging.Formatter("CONSOLE: %(levelname)s - %(name)s - %(message)s")
    console_handler.setFormatter(console_formatter)

    # Avoid duplicate handlers if already added
    if not logger.hasHandlers():
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

    return logger




#module 1:
from logger_setup import setup_logger

logger = setup_logger("module_a", "module_a.log")

def process_data():
    logger.info("Processing data in module A...")
    logger.warning("This is a warning from module A!")



#main
import module_a
import module_b
from logger_setup import get_logger

logger = get_logger(__name__)

if __name__ == "__main__":
    logger.info("Starting the application...")
    module_a.process_data()
    module_b.fetch_data()
    logger.info("Application finished.")"""""