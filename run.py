from src.consumer import ImageConsumer
from src.utils.logger import setup_logger

if __name__ == "__main__":
    logger = setup_logger()
    logger.info("Starting application...")
    
    try:
        print()
        consumer = ImageConsumer()
        consumer.run()
    except Exception as e:
        logger.error(f"Application failed: {e}", exc_info=True)