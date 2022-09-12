import argparse
import json
from os import path, makedirs

import pika

from logger import Logger

log = Logger()

def get_argument_parser():
    """
    Parses for the following arguments:
    1.  cloud-amqp-config
    2.  database-config
    3.  oanda-config
    4.  output-path
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("cloud-amqp-config", help="Location to cloud AMQP configuration (json) file")
    parser.add_argument("database-config", help="Location to database configuration (json) file")
    parser.add_argument("oanda-config", help="Location to oanda configuration (json) file")
    parser.add_argument("save-path", help="Folder location to save downloaded files")
    return parser

def get_amqp_url_parameters(config_file_path):
            
    full_path = path.abspath(config_file_path)
    
    if not path.exists(full_path):
        log.error(f"Path {full_path} does not exists.")
        exit(2)
    
    try:
        with open(full_path, "r", encoding="utf-8") as in_file:
            json_data = json.load(in_file)
    except Exception as e:
        log.error(e)
        exit(3)
    
    if 'cloud_amqp' not in json_data \
        or 'armadillo' not in json_data['cloud_amqp'] \
        or'url' not in json_data['cloud_amqp']['armadillo']:
        log.error("Config file does not have structure [ cloud_amqp > armadillo > url ]")
        exit(4)
    
    cloud_amqp_url = json_data['cloud_amqp']['armadillo']['url']

    log.info("Cloud AMQP URL read", source="program", event="set", target="cloud amqp url")
    
    return pika.URLParameters(cloud_amqp_url)


def get_database_settings(config_file_path):
    
    full_path = path.abspath(config_file_path)
    
    if not path.exists(full_path):
        log.error(f"ERROR - output path does not exists {full_path}")
        exit(2)

    with open(full_path, 'r', encoding='utf-8') as in_file:
        mysql_settings = json.loads(in_file.read())

    return mysql_settings

def get_oanda_settings(config_file_path):
            
    full_path = path.abspath(config_file_path)
    
    if not path.exists(full_path):
        log.error(f"Path {full_path} does not exists.")
        exit(2)
    
    try:
        with open(full_path, "r", encoding="utf-8") as in_file:
            json_data = json.load(in_file)
    except Exception as e:
        log.error(e)
        exit(3)

    expected_keys = [ 'account_number', 'api_key', 'rest_api_url', 'streaming_api_url' ]
    has_unexpected_key = False in [ json_data_key in expected_keys for json_data_key in json_data.keys() ]
    
    if has_unexpected_key:
        log.error(f"Config file does not proper structure; should have {expected_keys}")
        exit(4)

    log.info("Oanda settings read", source="program", event="set", target="oanda settings")
    
    return json_data

def get_save_file_full_path(directory_path):

    full_path = path.abspath(directory_path)
    
    if not path.exists(full_path):
        try:
            makedirs(full_path)
        except Exception:
            log.error(f"ERROR - output path does not exists {full_path}")
            exit(2)

    return full_path


def get_settings_from_arguments():
    parser = get_argument_parser()
    args = vars(parser.parse_args())
    cloud_amqp_config_file_path = args['cloud-amqp-config']
    database_config_file_path = args['database-config']
    oanda_config_file_path = args['oanda-config']
    save_file_directory = args['save-path']
    return (get_amqp_url_parameters(cloud_amqp_config_file_path),
        get_database_settings(database_config_file_path),
        get_oanda_settings(oanda_config_file_path),
        get_save_file_full_path(save_file_directory))