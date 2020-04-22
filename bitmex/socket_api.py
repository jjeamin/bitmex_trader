import sys
import ssl
import time
import json
import websocket
import logging
import threading
from bitmex import api_key
import decimal
import traceback
from future.utils import iteritems
from src.bitmex_utils import findItemByKeys, toNearest
from urllib.parse import urlparse, urlunparse

"""
########################################################################################################################
########################################################################################################################
###################################################### Market ##########################################################
########################################################################################################################
########################################################################################################################
"""


class BitMexWebSocket(object):
    MAX_TABLE_LEN = 200

    def __init__(self, api_key, api_secret, symbol):
        self.logger = logging.getLogger('root')
        self.api_key = api_key
        self.api_secret = api_secret
        self.symbol = symbol
        self.__reset()

    def __del__(self):
        self.exit()

    def connect(self, endpoint="", shouldAuth=True):
        """
        WebSocket connect
        """

        self.logger.debug("CONNECT SOCKET")
        self.shouldAuth = shouldAuth

        subscriptions = [sub + ':' + self.symbol for sub in ["quote", "trade"]]
        subscriptions += ["instrument"]

        if self.shouldAuth:
            subscriptions += [sub + ':' + self.symbol for sub in ["order", "execution", "orderBook10"]]
            subscriptions += ["margin", "position"]

        urlParts = list(urlparse(endpoint))
        urlParts[0] = urlParts[0].replace('http', 'ws')
        urlParts[2] = "/realtime?subscribe=" + ",".join(subscriptions)
        url = urlunparse(urlParts)
        self.logger.info("Connecting to %s" % url)
        self.__connect(url)
        self.logger.info('Connected to WS. Waiting for data images, this may take a moment...')

        # Connected. Wait for partials
        self.__wait_for_symbol()
        if self.shouldAuth:
            self.__wait_for_account()
        self.logger.info('Got all market data. Starting.')

    def get_instrument(self):
        """
        Get instrument
        """
        instruments = self.data['instrument']
        matchingInstruments = [i for i in instruments if i['symbol'] == self.symbol]
        if len(matchingInstruments) == 0:
            raise Exception("Unable to find instrument or index with symbol: " + self.symbol)
        instrument = matchingInstruments[0]
        # Turn the 'tickSize' into 'tickLog' for use in rounding
        # http://stackoverflow.com/a/6190291/832202
        instrument['tickLog'] = decimal.Decimal(str(instrument['tickSize'])).as_tuple().exponent * -1
        return instrument

    def get_ticker(self):
        """
        Get Ticker
        """
        instrument = self.get_instrument()

        # If this is an index, we have to get the data from the last trade.
        if instrument['symbol'][0] == '.':
            ticker = {}
            ticker['mid'] = ticker['buy'] = ticker['sell'] = ticker['last'] = instrument['markPrice']
        # Normal instrument
        else:
            bid = instrument['bidPrice'] or instrument['lastPrice']
            ask = instrument['askPrice'] or instrument['lastPrice']
            ticker = {
                "last": instrument['lastPrice'],
                "buy": bid,
                "sell": ask,
                "mid": (bid + ask) / 2
            }

        # The instrument has a tickSize. Use it to round values.
        return {k: toNearest(float(v or 0), instrument['tickSize']) for k, v in iteritems(ticker)}

    def funds(self):
        return self.data['margin'][0]

    def market_depth(self):
        return self.data['orderBook10'][0]

    def open_orders(self, clOrdIDPrefix):
        orders = self.data['order']
        # Filter to only open orders (leavesQty > 0) and those that we actually placed
        return [o for o in orders if str(o['clOrdID']).startswith(clOrdIDPrefix) and o['leavesQty'] > 0]

    def position(self):
        positions = self.data['position']
        pos = [p for p in positions if p['symbol'] == self.symbol]
        if len(pos) == 0:
            # No position found; stub it
            return {'avgCostPrice': 0, 'avgEntryPrice': 0, 'currentQty': 0, 'symbol': self.symbol}
        return pos[0]

    def recent_trades(self):
        return self.data['trade']

    def __get_auth(self):
        """
        Get Auth
        Setting API KEY
        """
        if self.shouldAuth is False:
            return []

        self.logger.info("Authenticating with API Key.")
        # To auth to the WS using an API key, we generate a signature of a nonce and
        # the WS API endpoint.
        nonce = api_key.generate_expires()
        return [
            "api-expires: " + str(nonce),
            "api-signature: " + api_key.generate_signature(self.api_secret, 'GET', '/realtime', nonce, ''),
            "api-key:" + self.api_key
        ]

    def __connect(self, url):
        """
        WebSocket Thread connect
        """
        self.logger.debug("Starting thread")

        ssl_defaults = ssl.get_default_verify_paths()
        sslopt_ca_certs = {'ca_certs': ssl_defaults.cafile}

        self.ws = websocket.WebSocketApp(url,
                                         on_message=self.__on_message,
                                         on_open=self.__on_open,
                                         on_close=self.__on_close,
                                         on_error=self.__on_error,
                                         header=self.__get_auth())

        self.wst = threading.Thread(target=lambda: self.ws.run_forever(sslopt=sslopt_ca_certs))
        self.wst.daemon = True
        self.wst.start()
        self.logger.info("Started thread")

        conn_timeout = 5

        while (not self.ws.sock or not self.ws.sock.connected) and conn_timeout and not self._error:
            time.sleep(1)
            conn_timeout -= 1

        if not conn_timeout or self._error:
            self.logger.error("Couldn't connect to WS! Exiting.")
            self.exit()
            sys.exit(1)

    def error(self, err):
        """
        Error
        """
        self._error = err
        self.logger.error(err)
        self.exit()

    def exit(self):
        """
        Exit
        """
        self.exited = True
        self.ws.close()

    def __on_message(self, message):
        """
        WebSocket Message
        """
        message = json.loads(message)
        self.logger.debug(json.dumps(message))

        table = message['table'] if 'table' in message else None
        action = message['action'] if 'action' in message else None

        try:
            if 'subscribe' in message:
                if message['success']:
                    self.logger.debug("Subscribed to %s." % message['subscribe'])
                else:
                    self.error("Unable to subscribe to %s. Error: \"%s\" Please check and restart." %
                               (message['request']['args'][0], message['error']))
            elif 'status' in message:
                if message['status'] == 400:
                    self.error(message['error'])
                if message['status'] == 401:
                    self.error("API Key incorrect, please check and restart.")
            elif action:

                if table not in self.data:
                    self.data[table] = []

                if table not in self.keys:
                    self.keys[table] = []

                # There are four possible actions from the WS:
                # 'partial' - full table image
                # 'insert'  - new row
                # 'update'  - update row
                # 'delete'  - delete row
                if action == 'partial':
                    self.logger.debug("%s: partial" % table)
                    self.data[table] += message['data']
                    # Keys are communicated on partials to let you know how to uniquely identify
                    # an item. We use it for updates.
                    self.keys[table] = message['keys']

                elif action == 'insert':
                    self.logger.debug('%s: inserting %s' % (table, message['data']))
                    self.data[table] += message['data']

                    # Limit the max length of the table to avoid excessive memory usage.
                    # Don't trim orders because we'll lose valuable state if we do.
                    if table not in ['order', 'orderBookL2'] and len(self.data[table]) > BitMexWebSocket.MAX_TABLE_LEN:
                        self.data[table] = self.data[table][(BitMexWebSocket.MAX_TABLE_LEN // 2):]

                elif action == 'update':
                    self.logger.debug('%s: updating %s' % (table, message['data']))
                    # Locate the item in the collection and update it.
                    for updateData in message['data']:
                        item = findItemByKeys(self.keys[table], self.data[table], updateData)
                        if not item:
                            continue  # No item found to update. Could happen before push

                        # Log executions
                        if table == 'order':
                            is_canceled = 'ordStatus' in updateData and updateData['ordStatus'] == 'Canceled'
                            if 'cumQty' in updateData and not is_canceled:
                                contExecuted = updateData['cumQty'] - item['cumQty']

                                if contExecuted > 0:
                                    instrument = self.get_instrument()
                                    self.logger.info("Execution: %s %d Contracts of %s at %.*f" %
                                                     (item['side'], contExecuted, item['symbol'],
                                                      instrument['tickLog'], item['price']))

                        # Update this item.
                        item.update(updateData)

                        # Remove canceled / filled orders
                        if table == 'order' and item['leavesQty'] <= 0:
                            self.data[table].remove(item)

                elif action == 'delete':
                    self.logger.debug('%s: deleting %s' % (table, message['data']))
                    # Locate the item in the collection and remove it.
                    for deleteData in message['data']:
                        item = findItemByKeys(self.keys[table], self.data[table], deleteData)
                        self.data[table].remove(item)
                else:
                    raise Exception("Unknown action: %s" % action)
        except:
            self.logger.error(traceback.format_exc())

    def __on_open(self):
        """
        WebSocket Open
        """
        self.logger.debug("OPEN SOCKET")

    def __on_close(self):
        """
        WebSocket Close
        """
        self.logger.info("CLOSE SOCKET")
        self.exit()

    def __on_error(self, error):
        """
        WebSocket Error
        """
        if not self.exited:
            self.error(error)

    def __wait_for_account(self):
        """
        Subscribe Wait Account
        """
        while not {'margin', 'position', 'order', 'orderBook10'} <= set(self.data):
            time.sleep(0.1)

    def __wait_for_symbol(self):
        """
        Subscribe Wait Symbol
        """
        while not {'instrument', 'trade', 'quote'} <= set(self.data):
            time.sleep(0.1)

    def __send_command(self, command, args):
        """
        Send Command
        """
        self.ws.send(json.dumps({"op": command, "args": args or []}))

    def __reset(self):
        """
        Reset
        """
        self.data = {}
        self.keys = {}
        self.exited = False
        self._error = None
