import sqlite3

def init_db():
    conn = sqlite3.connect('rides.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS rides 
                 (id TEXT PRIMARY KEY, user_id INTEGER, km REAL, status TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS outbox 
                 (id INTEGER PRIMARY KEY AUTOINCREMENT, event_type TEXT, payload TEXT, published INTEGER DEFAULT 0)''')
    conn.commit()
    conn.close()

def create_ride(ride_id, user_id, km):
    conn = sqlite3.connect('rides.db')
    c = conn.cursor()
    c.execute("INSERT INTO rides VALUES (?, ?, ?, ?)", (ride_id, user_id, km, "IN_PROGRESS"))
    conn.commit()
    conn.close()

def finish_ride_transaction(ride_id, payload_json):
    conn = sqlite3.connect('rides.db')
    c = conn.cursor()
    try:
        print(f"[DB] Updating ride {ride_id} to PENDING_ANALYSIS")
        c.execute("UPDATE rides SET status = ? WHERE id = ?", ("PENDING_ANALYSIS", ride_id))
        
        print(f"[DB] Writing to Outbox for {ride_id}")
        c.execute("INSERT INTO outbox (event_type, payload) VALUES (?, ?)", ("ride.finished", payload_json))
        
        conn.commit() 
    except Exception as e:
        conn.rollback()
        print(f"Tx Error: {e}")
        return False
    finally:
        conn.close()

def compensate_ride(ride_id, reason):
    conn = sqlite3.connect('rides.db')
    c = conn.cursor()
    c.execute("UPDATE rides SET status = ? WHERE id = ?", (f"FAILED: {reason}", ride_id))
    conn.commit()
    conn.close()
    print(f"[SAGA] Compensation executed. Ride {ride_id} marked as FAILED.")