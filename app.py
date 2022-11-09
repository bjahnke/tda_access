import pandas as pd
import json
import tda_access.access as taa
import multiprocessing as mp
from time import perf_counter
import typing as t


def stream_app(credentials, stream_settings: t.Dict, send_conn):
    _client = taa.TdBrokerClient(credentials=credentials)
    _stream = _client.init_stream(
        live_quote_fp=stream_settings['live_quote_path'],
        price_history_fp=stream_settings['price_history_path'],
        interval=1,
        interval_type='d'
    )

    _stream.run_stream(send_conn, symbols=['AAPL', 'GOOGL', 'ABBV', 'AMZN', 'NFLX'])


def read_process(price_history_path, writer_receive_conn):
    data = pd.DataFrame()

    while True:
        if writer_receive_conn.poll():
            recv_start = perf_counter()
            _ = writer_receive_conn.recv()
            new_data = pd.read_csv(price_history_path)
            read_time = perf_counter() - recv_start
            if new_data.equals(data):
                continue
            else:
                data = new_data
                equals_check = perf_counter() - recv_start
                print(f'read time: {read_time}')
                print(f'equals check: {equals_check}')


"""
Json Structure
{ 
  "credentials": {
    "client": {
      "api_key": "",
      "redirect_uri": "https://localhost",
      "token_path": ""
    },
    "account_id": int
  },
  "paths": {
    "live_quote_path": "",
    "price_history_path": ""
  }
}
"""

if __name__ == '__main__':
    with open('..\\..\\..\\data_args\\credentials.json', 'r') as cred_file:
        _creds = json.load(cred_file)['credentials']

    with open('..\\..\\..\\data_args\\scan_args.json', 'r') as fp:
        _stream_settings = json.load(fp)['stream_settings']

    _receive_conn, _send_conn = mp.Pipe(duplex=True)
    _stream_process = mp.Process(target=stream_app, args=(_creds, _stream_settings, _send_conn,))
    _stream_process.start()

    read_process(_stream_settings['price_history_path'], _receive_conn)
    print('d')
