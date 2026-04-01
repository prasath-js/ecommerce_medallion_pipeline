import logging
import time

from flows.medallion_flow import ecommerce_medallion_pipeline_flow

# Configure basic logging for the main execution script
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main() -> None:
    """
    Main function to trigger the Prefect orchestration flow for the e-commerce medallion pipeline.
    """
    logger.info("Starting e-commerce medallion pipeline flow...")
    start_time = time.monotonic()

    try:
        # Run the Prefect flow
        state = ecommerce_medallion_pipeline_flow()

        if state.is_completed(): # Use state.is_completed() for Prefect 2.x
            logger.info("E-commerce medallion pipeline flow completed successfully!")
        else:
            logger.error(f"E-commerce medallion pipeline flow finished with state: {state.name}")

    except Exception as e:
        logger.critical(f"An unhandled error occurred during flow execution: {e}", exc_info=True)

    finally:
        end_time = time.monotonic()
        duration = end_time - start_time
        logger.info(f"Total pipeline execution time: {duration:.2f} seconds")


if __name__ == "__main__":
    main()
