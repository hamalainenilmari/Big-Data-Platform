from confluent_kafka import Consumer, Producer
import argparse
import json
import time

# kafka error
def kafka_delivery_error(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')

# Stream ingestion manager
def main():
    parser = argparse.ArgumentParser()
    parser = argparse.ArgumentParser()
    parser.add_argument('-b', '--broker', default="localhost:9092", help='Broker as "server:port"')
    parser.add_argument('-t', '--topics', nargs="+", default=["chicago_taxitrips"], help='tenants kafka topics')
    parser.add_argument('-g', '--consumer_group', default="manager", help='consumer group')
    
    args = parser.parse_args()
    broker = args.broker
    topics = args.topics
    
    # key: tenant id, value: tuple ( running status (true/false), seconds since last message)
    tenants = {}
    for item in topics:
        tenants[item.split("_")[0]] = (False, 0)

    print("tenants: ")
    print(tenants)

    #create configuration file for kafka connection
    kafka_conf = {
            'bootstrap.servers': broker,
            'group.id': args.consumer_group,
            }
        
    consumer = Consumer(kafka_conf)
    consumer.subscribe(topics)

    kafka_conf_producer={
            'bootstrap.servers': 'localhost:9092'
        } 

    # producer for sending msgs to starting / stopping pipelines
    producer = Producer(kafka_conf_producer)

    # listen to all tenants
    try:
        while True: # consume in a loop
            # check for new messages of tenants topics every 10 seconds
            msg = consumer.poll(10.0)

            if msg is None:
                # no new message
                for key, value in tenants.items():
                    tenants[key] = (value[0], value[1] + 10)
                    lastMessageTime = tenants[key][1]
                    print(f"Time since last tenant {key} message: {lastMessageTime} s")

                    if (lastMessageTime >= 60 and tenants[key][0]): 
                        # no new messages in 60 seconds and tenant is running, send message to stop tenant pipeline
                        msg = json.dumps({"action": "stop"}) # message to start pipeline execution
                        control_topic = f"{key}_ingestioncontrol" # corresponding topic
                        print(f"send message: {msg}, topic: {control_topic}")
                        producer.produce(control_topic, msg.encode('utf-8'), callback=kafka_delivery_error)
                        producer.flush()
                        tenants[key] = (False, lastMessageTime) # set tenant to not running
                continue

            if msg.error():
                print(f"Kafka error: {msg.error()}")
                continue

            if msg:
                # new message inbound
                topic = msg.topic()
                tenant = topic.split("_")[0]
                if (not tenants[tenant][0]): # if tenant not running, start execution
                    print(f"new msg for tenant {tenant}, not running -> start execution")

                    msg = json.dumps({"action": "start"}) # message to start pipeline execution
                    control_topic = f"{tenant}_ingestioncontrol" # corresponding topic

                    producer.produce(control_topic, msg.encode('utf-8'), callback=kafka_delivery_error)
                    producer.flush()
                    tenants[tenant] = (True, 0) # reset tenants time since last msg, set to running
                else: # execution already going on, no need to send message
                    tenants[tenant] = (True, 0) # reset tenants time since last msg, set to running
                    print("tenant already running")
    except Exception as e:
        print(f"Error: {e}")


if __name__ == '__main__':
    main()