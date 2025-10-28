import os
from dotenv import load_dotenv

load_dotenv()

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
NODE_ID = os.getenv("NODE_ID", "node1")
