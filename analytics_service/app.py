import pika
import json
import time

def send_compensation(ride_id, reason):
    connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
    channel = connection.channel()
    channel.queue_declare(queue='saga_compensations')
    
    msg = json.dumps({"ride_id": ride_id, "reason": reason})
    channel.basic_publish(exchange='', routing_key='saga_compensations', body=msg)
    connection.close()
    print(f"[Analytics] Sent COMPENSATION request for {ride_id}")

def callback(ch, method, properties, body):
    msg = json.loads(body)
    event_type = msg['type']
    data = msg['data']
    
    if event_type == 'ride.finished':
        ride_id = data['ride_id']
        print(f" [x] Processing stats for Ride: {ride_id}")
        time.sleep(2)
        
        if data.get('simulate_error'):
            print(f" [!] ERROR: Analytics failed for {ride_id} (Simulated)")
            send_compensation(ride_id, "Analytics Validation Failed")
        else:
            print(f" [v] SUCCESS: Stats updated for {ride_id}")

time.sleep(15)
connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
channel = connection.channel()
channel.queue_declare(queue='saga_events')

print(' [*] Analytics Worker Waiting...')
channel.basic_consume(queue='saga_events', on_message_callback=callback, auto_ack=True)
channel.start_consuming()