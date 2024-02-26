from multiprocessing import Pool
from itertools import chain
import os
from faker import Faker
from joblib import Parallel, delayed
import numpy as np
import pandas as pd
import random
from datetime import datetime
import pytz

fake = Faker()

# Define IST timezone
IST = pytz.timezone('Asia/Kolkata')

def generate_credit_card_transactions(_):
    transactions = []

    fake = Faker(locale='en_IN') if random.random() < 0.8 else Faker()

    # Generate a timestamp in IST
    timestamp = fake.date_time_between(start_date="-1y", end_date="now")
    timestamp = IST.localize(timestamp)

    location = fake.address()

    # Set currency based on location
    currency = 'INR' if location == 'India' else random.choice(['USD', 'EUR', 'GBP', 'JPY'])

    amount = round(random.uniform(1.0, 500000.0), 2)

    # Generate a high-value international transaction flag
    is_high_value_international = location != 'India' and amount > 275000

    trans_type = random.choice(['purchase', 'refund', 'withdrawal', 'transfer'])

    cate = random.choice(['food', 'clothing', 'transportation', 'entertainment', 'uncategorized'])

    needs_investigation = (cate == 'uncategorized' and (trans_type == 'transfer' or trans_type == 'withdrawal'))

    transaction = {
        'timestamp': timestamp,
        'amount': amount,  # Expanded range for high-value transactions
        'merchant': fake.company(),
        'card_number': fake.credit_card_number(card_type='visa'),
        'card_holder': fake.name(),
        'transaction_type': trans_type,
        'category': cate,
        'location': location,
        'country': location.split()[-1],  # Extract country from location
        'currency': currency,
        'device': fake.user_agent(),
        'ip_address': fake.ipv4(network=False),
        'latitude': round(random.uniform(-90, 90), 6),
        'longitude': round(random.uniform(-180, 180), 6),
        'is_flagged': is_high_value_international,
        'partner': fake.company(),
        'partner_type': random.choice(['bank', 'merchant', 'vendor', 'unidentified', 'individual']),
        'transaction_id': fake.uuid4(),
        'approval_code': fake.random_number(digits=6),
        'terminal_id': fake.random_number(digits=8),
        'customer_segment': random.choice(['premium', 'gold', 'silver', 'bronze']),
        'needs_investigation': 'Y' if needs_investigation else 'N' if not needs_investigation and random.random() < 0.2 else ""
    }
    transactions.append(transaction)

    return transactions

if __name__ == "__main__":
    num_transactions = 15000
    with Pool(processes=os.cpu_count()*2) as exe:
        result = exe.map(generate_credit_card_transactions, [_ for _ in range(15000)])
        results_unlisted = list(chain(*result))
    
    transactions_df = pd.DataFrame(results_unlisted)
    transactions_df.to_csv("credit_card_transactions_2.csv", index=False)