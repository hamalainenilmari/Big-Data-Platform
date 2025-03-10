from confluent_kafka import Consumer, Producer
import argparse
import json



def kafka_delivery_error(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')

# Stream ingestion manager
def main():
    parser = argparse.ArgumentParser()
    parser = argparse.ArgumentParser()
    parser.add_argument('-b', '--broker', default="localhost:9092", help='Broker as "server:port"')
    parser.add_argument('-t', '--topics', nargs="+", default=["taxiTrips"], help='tenants kafka topics')
    parser.add_argument('-g', '--consumer_group', default="manager", help='consumer group')
    
    args = parser.parse_args()
    broker = args.broker
    topics = args.topics
    
    # key: tenant id, value: running status (true/false)
    tenants = {}
    for item in topics:
        tenants[item.split("_")[0]] = False

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
    # producer for starting / stopping pipelines
    producer = Producer(kafka_conf_producer)

    # listen to all tenants
    try:
        while True: # consume in a loop
            msg = consumer.poll(5.0)
            if msg is None:
                continue
            if msg.error():
                print("error")
                continue
            if msg: 
                #topic = msg.topic()
                tenant = topic.split("_")[0]
                print("message from tenant: " + tenant)
                if not tenants[tenant]:
                    print("tenant not running, start execution")

                    msg = "{'action': 'start'}"
                    topic = f"{tenant}_ingestioncontrol"
                    print("topic: " + topic)
                    producer.produce(topic, msg.encode('utf-8'), callback=kafka_delivery_error)
                    producer.flush()
                else:
                    print("tenant already running")
    except Exception as e:
        print(f"Error: {e}")


if __name__ == '__main__':
    main()