import sqlite3
import random
import json

def seed_database():
    # Підключаємось до файлу БД (він створиться, якщо його немає)
    conn = sqlite3.connect('rides.db')
    c = conn.cursor()

    # На всяк випадок переконаємось, що таблиці існують (код з db.py)
    c.execute('''CREATE TABLE IF NOT EXISTS rides
                 (id TEXT PRIMARY KEY, user_id INTEGER, km REAL, status TEXT)''')
    
    c.execute('''CREATE TABLE IF NOT EXISTS outbox
                 (id INTEGER PRIMARY KEY AUTOINCREMENT, event_type TEXT, payload TEXT, published INTEGER DEFAULT 0)''')

    print("Generating 50 random rides...")

    for i in range(1, 51):
        # Генеруємо дані
        ride_id = f"seed_ride_{i}"
        user_id = random.randint(1, 100)
        km = round(random.uniform(5.0, 50.0), 2)
        
        # Формуємо payload для повідомлення
        payload = {
            "ride_id": ride_id,
            "user_id": user_id,
            "km": km
        }
        payload_json = json.dumps(payload)

        try:
            # 1. Записуємо поїздку в таблицю rides зі статусом PENDING_ANALYSIS
            c.execute("INSERT OR IGNORE INTO rides (id, user_id, km, status) VALUES (?, ?, ?, ?)",
                      (ride_id, user_id, km, "PENDING_ANALYSIS"))

            # 2. Записуємо подію в outbox (щоб вона полетіла в RabbitMQ)
            # event_type "ride.finished" відповідає тому, що очікує Analytics Service
            c.execute("INSERT INTO outbox (event_type, payload) VALUES (?, ?)",
                      ("ride.finished", payload_json))
            
            print(f"Added {ride_id} for User {user_id} ({km} km)")
        except Exception as e:
            print(f"Error inserting {ride_id}: {e}")

    conn.commit()
    conn.close()
    print("--- Database seeding completed! ---")

if __name__ == '__main__':
    seed_database()