import json
import logging

from datetime import datetime
from os import path
from urllib.parse import quote
from urllib.request import urlopen as url_open
from urllib.request import Request as url_request

import pika

from logger import Logger
from program_arguments import get_settings_from_arguments
from oanda_api import OandaApi



# 
# financial_instrument
# └───fetch.oanda.price
#     └───fetch_oanda_price

EXCHANGE_NAME = 'financial_instrument'
ROUTING_KEY = "fetch.oanda.price"
QUEUE_NAME = 'fetch_oanda_price'


def setup_logging():
    logging.getLogger('pika').setLevel(logging.WARNING)
    log = Logger()
    return log

def process_message(channel, method, properties, body):
    log.info("Received message")
    ticker = body.decode('utf-8')
    # oanda_api.get_latest_candles()
    # response_data = fetch_price_data(ticker)
    # if response_data is None:
    #     blacklist(ticker)
    # parse_and_publish_price_data(response_data)
    # channel.basic_ack(delivery_tag=method.delivery_tag)
    log.info("Message processed", source="program", event="processed", content=ticker)


def setup_rabbit_mq(channel):
    channel.queue_declare(
        queue=QUEUE_NAME, 
        durable=True)

    channel.basic_consume(
        queue=QUEUE_NAME, 
        on_message_callback=process_message)
    

def listen_for_tickers(url_parameters):
    with pika.BlockingConnection(url_parameters) as connection, connection.channel() as channel:
        setup_rabbit_mq(channel)
        log.info("Listen to queue", source="program", event="listen", target=QUEUE_NAME)
        channel.start_consuming()


if __name__ == "__main__":
    log = setup_logging()
    (url_parameters, database_settings, oanda_settings, output_path) = get_settings_from_arguments()
    oanda_api = OandaApi(oanda_settings, output_path)
    listen_for_tickers(url_parameters)
    log.info("Program complete", source="program", event="complete")
