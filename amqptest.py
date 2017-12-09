#!/usr/bin/env python
import pika


def switch(dev, message):
    credentials = pika.PlainCredentials('rbccps', 'rbccps@123')
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='10.156.14.6',
                                                                   credentials=credentials))
    channel = connection.channel()
    channel.exchange_declare(exchange='amq.topic',
                             exchange_type='topic',
                             durable=True)
    channel.basic_publish(exchange='amq.topic',
                          routing_key='rbccps_sw'+dev,
                          body=message)
    connection.close()
