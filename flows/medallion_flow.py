import logging
from typing import Dict, Any

from prefect import flow

from pipeline.bronze import run_bronze_layer
from pipeline.silver import run_silver_layer
from pipeline.gold import run_gold_layer

# Configure logger for the flow
logger = logging.getLogger(__name__)

@flow(name="ecommerce_medallion_pipeline_flow", log_prints=True, retries=1, retry_delay_seconds=30)
def ecommerce_medallion_pipeline_flow() -> Dict[str, Any]:
    """
    Main Prefect flow for orchestrating the e-commerce medallion data pipeline.
    
    The flow consists of three main stages: Bronze, Silver, and Gold, each processing
    data sequentially to build up the medallion architecture.
    
    Returns:
        Dict[str, Any]: A dictionary containing the overall status and results from each layer.
    """
    logger.info("Starting e-commerce medallion pipeline flow orchestration.")
    flow_results = {}

    # 1. Run Bronze Layer
    try:
        bronze_result = run_bronze_layer()
        flow_results["bronze_layer"] = bronze_result
        if bronze_result.get("status") != "completed":
            logger.error("Bronze layer did not complete successfully. Aborting Silver and Gold layers.")
            return {"status": "failed", "message": "Bronze layer failed", "details": flow_results}
        logger.info(f"Bronze layer completed. Details: {bronze_result}")
    except Exception as e:
        logger.critical(f"Bronze layer failed with an unhandled exception: {e}", exc_info=True)
        return {"status": "failed", "message": "Bronze layer critical failure", "details": flow_results}

    # 2. Run Silver Layer (depends on Bronze layer completion)
    try:
        silver_result = run_silver_layer(wait_for=[bronze_result]) # Explicit dependency
        flow_results["silver_layer"] = silver_result
        if silver_result.get("status") != "completed":
            logger.error("Silver layer did not complete successfully. Aborting Gold layer.")
            return {"status": "failed", "message": "Silver layer failed", "details": flow_results}
        logger.info(f"Silver layer completed. Details: {silver_result}")
    except Exception as e:
        logger.critical(f"Silver layer failed with an unhandled exception: {e}", exc_info=True)
        return {"status": "failed", "message": "Silver layer critical failure", "details": flow_results}

    # 3. Run Gold Layer (depends on Silver layer completion)
    try:
        gold_result = run_gold_layer(wait_for=[silver_result]) # Explicit dependency
        flow_results["gold_layer"] = gold_result
        if gold_result.get("status") != "completed":
            logger.error("Gold layer did not complete successfully.")
            return {"status": "failed", "message": "Gold layer failed", "details": flow_results}
        logger.info(f"Gold layer completed. Details: {gold_result}")
    except Exception as e:
        logger.critical(f"Gold layer failed with an unhandled exception: {e}", exc_info=True)
        return {"status": "failed", "message": "Gold layer critical failure", "details": flow_results}

    logger.info("E-commerce medallion pipeline flow finished successfully!")
    return {"status": "success", "message": "All layers completed", "details": flow_results}
