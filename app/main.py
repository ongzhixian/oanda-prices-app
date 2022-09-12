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
ROUTING_KEY = 'analyse.oanda.price'
OUT_QUEUE_NAME = 'analyse_oanda_price'
IN_QUEUE_NAME = 'fetch_oanda_price'


def setup_logging():
    logging.getLogger('pika').setLevel(logging.WARNING)
    log = Logger()
    return log

def setup_rabbit_mq_for_output(channel):
    channel.exchange_declare(
        exchange=EXCHANGE_NAME, 
        exchange_type='topic')
        
    channel.queue_declare(
        queue=OUT_QUEUE_NAME, 
        durable=True)
    
    channel.queue_bind(
        exchange=EXCHANGE_NAME, 
        routing_key=f"{ROUTING_KEY}.*",
        queue=OUT_QUEUE_NAME)

def publish_prices_for_analysis(url_parameters, historical_candle_json, ticker):

    with pika.BlockingConnection(url_parameters) as connection, connection.channel() as channel:

        setup_rabbit_mq_for_output(channel)

        channel.basic_publish(
            exchange=EXCHANGE_NAME, 
            routing_key=f"{ROUTING_KEY}.{ticker}", 
            body=json.dumps(historical_candle_json),
            properties=pika.BasicProperties(
                delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE
            ))

        log.info(f"Publish {ticker}", event="publish", type="ticker", target=ticker)
        

def process_message(channel, method, properties, body):
    log.info("Received message")
    ticker = body.decode('utf-8')

    # KIV:Get latest candles
    # candle_spec='EUR_USD:S10:BM'
    # candles = oanda_api.get_latest_candles(candle_spec)

    historical_candle_json = oanda_api.get_historical_candles(ticker)
    publish_prices_for_analysis(url_parameters, historical_candle_json, ticker)
    # channel.basic_ack(delivery_tag=method.delivery_tag)
    log.info("Message processed", source="program", event="processed", content=ticker)


def setup_rabbit_mq_for_input(channel):
    channel.queue_declare(
        queue=IN_QUEUE_NAME, 
        durable=True)

    channel.basic_consume(
        queue=IN_QUEUE_NAME, 
        on_message_callback=process_message)
    

def listen_for_tickers(url_parameters):
    with pika.BlockingConnection(url_parameters) as connection, connection.channel() as channel:
        setup_rabbit_mq_for_input(channel)
        log.info("Listen to queue", source="program", event="listen", target=IN_QUEUE_NAME)
        channel.start_consuming()


if __name__ == "__main__":
    log = setup_logging()
    (url_parameters, database_settings, oanda_settings, output_path) = get_settings_from_arguments()
    oanda_api = OandaApi(oanda_settings, output_path)
    listen_for_tickers(url_parameters)
    log.info("Program complete", source="program", event="complete")
