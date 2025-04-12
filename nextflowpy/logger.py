import os
import logging

os.makedirs(".nextflowpy", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(".nextflowpy/nextflowpy.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger("nextflowpy")