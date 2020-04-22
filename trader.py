import pytz
import time
import argparse
from datetime import datetime
from bitmex.socket_api import BitMexWebSocket
from bitmex.swagger_api import bitmex_api
from utils import get_minute_data, get_action


API_KEY = "[YOUR API KEY]"
API_SECRET = "[YOUT API SECRET]"

parser = argparse.ArgumentParser(description="BITMEX TRADER")

parser.add_argument('--symbol', '-s', type=str, default="XBTUSD")
parser.add_argument('--test', action='store_true', help='real / testnet')

args = parser.parse_args()

if args.test:
    url = "https://testnet.bitmex.com/api/v1"
else:
    url = "https://www.bitmex.com/api/v1"

# WebSocket
ws = BitMexWebSocket(API_KEY, API_SECRET, args.symbol)
ws.connect(url, args.symbol)

# Restful API
client = bitmex_api(test=args.test, api_key=API_KEY, api_secret=API_SECRET)

# parameter
datetime_minute_cached = None
position = 1

# order
while ws.ws.sock.connected:
    try:
        if datetime_minute_cached != datetime.now().minute:
            cur_time = datetime.now(pytz.timezone('Asia/Seoul'))

            df = get_minute_data(client, args.symbol, minutes=1, cur_time=cur_time)

            action = get_action(df)

            if action == 1 and position == 1:
                # market_order(client, args.symbol, "buy", args.amount)

                position = -1
                print("BUY")

            elif action == -1 and position == -1:
                # market_order(client, args.symbol, "sell", args.amount)

                position = 1
                print("SELL")

            else:
                print("HOLD")

            datetime_minute_cached = datetime.now().minute
            time.sleep(10)

    except Exception as e:
        print(str(e))
        ws.exit()
        print(" This is the end !")

    except KeyboardInterrupt:
        ws.exit()
        print(" This is the end !")
