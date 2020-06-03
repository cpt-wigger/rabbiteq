import pika


class Producer(object):

    def __init__(self, host, username, password):
        self.host = host
        self.username = username
        self.password = password
        self.connection_credentials = pika.PlainCredentials(self.username, self.password)
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=self.host, credentials=self.connection_credentials))
        self.channel = self.connection.channel()

    def init_queue(self, routing='default_queue', durable=True):
        self.channel.queue_declare(queue=routing, durable=durable)

    def put(self, body, routing='default_queue', durable=True):
        # durable 是否进行持久存储
        if durable:
            properties = pika.BasicProperties(delivery_mode=2)
            self.channel.basic_publish(exchange='', routing_key=routing, body=body, properties=properties)
        else:
            self.channel.basic_publish(exchange='', routing_key=routing, body=body)

    def close(self):
        self.connection.close()

    def __del__(self):
        try:
            self.connection.close()
        except:
            pass


class Consumer(object):
    def __init__(self, host, username, password, prefetch=1):
        self.host = host
        self.username = username
        self.password = password
        self.connection_credentials = pika.PlainCredentials(self.username, self.password)
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=self.host, credentials=self.connection_credentials))
        self.channel = self.connection.channel()
        # 开启公平分配，代替轮流分配，即设置每次分配的任务个数
        if prefetch:
            self.channel.basic_qos(prefetch_count=prefetch)

    def start_work(self, callback, routing='default_queue', auto_ack=True):
        self.channel.basic_consume(on_message_callback=callback, queue=routing, auto_ack=auto_ack)
        self.channel.start_consuming()

