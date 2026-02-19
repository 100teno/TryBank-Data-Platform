import subprocess
import sys
import logging
from datetime import datetime
import os

LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

log_filename = f"{LOG_DIR}/pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler(sys.stdout)
    ]
)

def run_step(script_name):
    logging.info(f"Running step: {script_name}")

    result = subprocess.run([sys.executable, f"jobs/{script_name}"])

    if result.returncode != 0:
        logging.error(f"Error while running {script_name}")
        sys.exit(1)

    logging.info(f"{script_name} completed successfully")
    

def main():
    logging.info("Starting the data pipeline")

    run_step("bronze_to_silver.py")
    run_step("silver_to_gold.py")

    logging.info("Pipeline completed successfully")

if __name__ == "__main__":
    main()