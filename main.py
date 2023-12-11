import faker
import psycopg2
from datetime import datetime
import random

fake = faker.Fake()

def generate_transaction():
    user = fake.simple_profile()
    
    return {
        "transactionId": fake.uuid4(),
        "userId": user["username"],
        "timestamp": datetime.utcnow().timestamp(),
        "amount": round(random.uniform(10, 1000), 2),
        "currency": random.choice(["USD", "GBR"]),
        "city": fake.city(),
        "country": fake.country(),
        "merchanName": fake.company(),
        "paymentMethod": random.choice(["credit_card", "debit_card", "online_transfer"]),
        "ipAddress": fake.ipv4(),
        "voucherCode": random.choice(["", "DISCOUNT10", ""]),
        "affiliateId": fake.uuid4()
        
    }
    
def create_table(conn):
    cursor = conn.cursor()
    
    cursor.execute(
    """
        CREATE TABLE IF NOT EXISTS transactions (
            transaction_id VARCGAR(255) PRIMARY KEY,
            user_id VARCGAR(255),
            timestamp TIMESTAMP,
            amount DECIMAL,
            currency VARCGAR(255),
            city VARCGAR(255),
            country VARCGAR(255),
            merchant_name VARCGAR(255),
            payment_method VARCGAR(255),
            ip_address VARCGAR(255),
            voucher_code VARCGAR(255),
            affiliateId VARCGAR(255)
        )
    """
    )
    cursor.close()
    conn.commit()
    
if __name__ == "__main__":
    conn = psycopg2.connect(
        host="localhost",
        database="finantial_db",
        user="postgres",
        password="postgres",
        port=5432
    )
    
    create_table(conn)
    
    transaction = generate_transaction()
    cur = conn.cursor()
    print(transaction)
    
    cur.execute(
    """
        INSERT INTO transactions(transaction_id, user_id, timestamp, amount, currency, city,
        country, merchant_name, payment_method, ip_address, affiliate_id, voucher_code)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (transaction["transaction_id"], transaction["userId"],transaction["timestamp"],transaction["amount"],
          transaction["currency"],transaction["city"],transaction["country"],transaction["merchantName"],
          transaction["paymentMethod"],transaction["ipAddress"],transaction["affiliateId"],transaction["voucherCode"],)
    )