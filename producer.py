import time
import json
import random
from datetime import datetime, timezone
from faker import Faker
from confluent_kafka import Producer

conf = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'financial-transaction-producer'
}

producer = Producer(conf)

Merchant_Categories = ["grocery", "fast food", "gas_station", "travel", "entertainment", "clothing", "electronics", "healthcare", "home_goods", "beauty", "sports", "education", "utilities", "restaurants", "coffee_shops", "pharmacies", "pet_supplies", "furniture", "jewelry", "toys", "books", "music", "movies", "gaming", "fitness", "outdoor_recreation", "auto_repair", "insurance", "real_estate", "legal_services", "financial_services", "telecommunications", "transportation", "charity", "government_services", "professional_services", "childcare", "elderly_care", "home_improvement", "landscaping", "cleaning_services", "event_planning", "photography", "art_supplies", "crafts", "hobbies", "travel_agencies", "airlines", "hotels", "car_rentals"]
Transaction_Types = ["POS", "ACH", "WIRE", "ATM", "P2P",]
Currencies = {
    'Mexico': 'MXN',
    'Canada': 'CAD',
    'United Kingdom': 'GBP',
    'France': 'EUR',
    'Germany':'EUR',
    'Italy': 'EUR',
    'Japan': 'JPY',
    'China': 'CNY'
}
foreign_fakers = {
    "Mexico": Faker('es_MX'),
    "Canada": Faker('en_CA'),
    "United Kingdom": Faker('en_GB'),
    "France": Faker('fr_FR'),
    "Germany": Faker('de_DE'),
    "Italy": Faker('it_IT'),
    "Japan": Faker('ja_JP'),
    "China": Faker('zh_CN')
}

def delivery_report(err, meg):
    if err is not None:
        print(f"Delivery failed for record {meg.key()}: {err}")
    else:
        print(f"Record {meg.key()} successfully produced to {meg.topic()} [{meg.partition()}] at offset {meg.offset()}")

def send_transactions(generate_banking_transaction, num_transactions=100):
    for _ in range(num_transactions):
        transaction = generate_banking_transaction()
        value_json = json.dumps(transaction).encode('utf-8')
        key = str(transaction['transaction_id']).encode('utf-8')
        producer.produce(topic='financial-transactions', key=key, value=value_json, callback=delivery_report)
        time.sleep(0.5)
        producer.poll(0)
        producer.flush()

def generate_banking_transaction():
    fake = Faker()

    is_fraudulent = random.choices([True, False], weights=[5, 95])[0]

    if is_fraudulent:
        amount = round(random.uniform(1500.00, 9500.00), 2)
        is_international = True

    else:
        amount = round(random.uniform(5.00, 350.00), 2)
        is_international = random.choices([True, False], weights=[20, 80])[0]

    if is_international:
        country_name, selected_fake =  random.choice(list(foreign_fakers.items()))
        merchant_address = selected_fake.address().replace("\n", ", ")
        merchant_country = country_name
        currency = Currencies[country_name]

    else:
        merchant_address = fake.address()
        merchant_country = 'USA'
        currency = 'USD'

    transaction = {
        "transaction_id": fake.uuid4(),
        "transaction_type": random.choice(Transaction_Types),
        "timestamp": datetime.now().isoformat(),
        "account_number": round(random.uniform(127198900000, 127198900050)),
        "amount": round(random.uniform(1.0, 1000.0), 2),
        "currency": currency,
        "merchant_name": fake.company(),
        "merchant_category": random.choice(Merchant_Categories),
        "merchant_address": merchant_address,
        "merchant_country": merchant_country,
        "is_international": is_international,
        "fraud_score": round(random.uniform(0.85, 0.99), 2) if is_fraudulent else round(random.uniform(0.01, 0.20), 2),
        }
    
    return transaction

if __name__ == "__main__":
    print("Starting Core Banking Transaction Stream Simulation...\n")
    send_transactions(generate_banking_transaction, num_transactions=100)
