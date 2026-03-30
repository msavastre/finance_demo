import json
import random
import time
import uuid
from datetime import datetime

import os
import sys

from google.cloud import bigquery

# Resolve local src/ folder if we are not running from app root directly
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(os.path.join(ROOT, "src"))

from rwa_demo.config import settings

def main():
    client = bigquery.Client(project=settings.project_id)
    dataset = settings.dataset
    table_id = f"{settings.project_id}.{dataset}.simulated_transactions"

    print(f"Starting transaction simulation into {table_id}...")

    # We use some predictable cardholder IDs for demo persistence
    cardholders = [
        {"id": "CH-1001", "limit": 5000},
        {"id": "CH-1002", "limit": 2000},
        {"id": "CH-1003", "limit": 10000},
        {"id": "CH-1004", "limit": 1500},
    ]

    # Insert loop
    for _ in range(20): # Simulate 20 swipes for the demo run
        ch = random.choice(cardholders)
        # Spike a few transactions to be breaches!
        is_breach = random.random() < 0.3
        amount = random.randint(100, 2000)
        if is_breach:
            amount = ch["limit"] + random.randint(100, 500)

        row = {
            "transaction_id": f"TX-{uuid.uuid4().hex[:10].upper()}",
            "cardholder_id": ch["id"],
            "transaction_amount": amount,
            "credit_limit": ch["limit"],
            "transaction_time": datetime.utcnow().isoformat() + "Z",
        }

        errors = client.insert_rows_json(table_id, [row])
        if errors:
            print(f"Errors inserting row: {errors}")
        else:
            print(f"Streamed transaction for {ch['id']}: ${amount} (Limit: ${ch['limit']})")

        time.sleep(2) # Wait 2 seconds between swipes

    print("Transaction simulation complete!")

if __name__ == "__main__":
    main()
