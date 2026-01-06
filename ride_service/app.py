from flask import Flask, request, jsonify
import json
import time
import threading
import pika
import db
import sqlite3

app = Flask(__name__)

db.init_db()

def outbox_relay():
    while True:
        try:
            conn = sqlite3.connect('rides.db')
            c = conn.cursor()
            c.execute("SELECT id, event_type, payload FROM outbox WHERE published = 0")
            events = c.fetchall()
            
            if events:
                mq_conn = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
                channel = mq_conn.channel()
                channel.queue_declare(queue='saga_events')
                
                for row in events:
                    evt_id, evt_type, payload = row
                    message = json.dumps({"type": evt_type, "data": json.loads(payload)})
                    channel.basic_publish(exchange='', routing_key='saga_events', body=message)
                    print(f"[Relay] Event {evt_id} published to RabbitMQ")
                    
                    c.execute("UPDATE outbox SET published = 1 WHERE id = ?", (evt_id,))
                
                mq_conn.close()
                conn.commit()
            conn.close()
        except Exception as e:
            print(f"[Relay Error] {e}")
        time.sleep(5)

def compensation_listener():
    while True:
        try:
            print("[SAGA] Connecting to RabbitMQ for compensation...")
            connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
            channel = connection.channel()
            channel.queue_declare(queue='saga_compensations')

            def callback(ch, method, properties, body):
                msg = json.loads(body)
                ride_id = msg['ride_id']
                reason = msg['reason']
                print(f"[SAGA] Received failure signal for {ride_id}")
                db.compensate_ride(ride_id, reason)

            print("[SAGA] Listening for compensations...")
            channel.basic_consume(queue='saga_compensations', on_message_callback=callback, auto_ack=True)
            channel.start_consuming()
        except Exception as e:
            print(f"[SAGA Error] Compensation Listener Error: {e}")
        time.sleep(5)

@app.route('/create', methods=['POST'])
def create():
    data = request.json
    db.create_ride(data['ride_id'], data['user_id'], data['km'])
    return jsonify({"status": "Created"})

@app.route('/finish', methods=['POST'])
def finish():
    data = request.json
    success = db.finish_ride_transaction(data['ride_id'], json.dumps(data))
    
    if success:
        return jsonify({"status": "Accepted", "saga_state": "PENDING_ANALYSIS"})
    else:
        return jsonify({"error": "Database Error"}), 500

threading.Thread(target=outbox_relay, daemon=True).start()
threading.Thread(target=compensation_listener, daemon=True).start()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5002)