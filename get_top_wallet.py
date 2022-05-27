import tornado.ioloop
import tornado.web
from tornado import options
import json
import requests
from multiprocessing import Process
import psycopg2
from time import sleep


class CentrifugoConnectHandler(tornado.web.RequestHandler):

    def check_xsrf_cookie(self):
        pass

    def post(self):
        if self.request:
            self.set_header('Content-Type', 'application/json; charset="utf-8"')

            file = open('key.txt')
            key = file.readline()

            if self.request.headers['X-ACCESS-TOKENls'] == key:
                top = self.request.body.decode().split(' ')[1].replace('}', '')

                data = get_top(top)

                self.write(data)


def main():
    options.parse_command_line()
    app = tornado.web.Application([
      (r'/centrifugo/connect', CentrifugoConnectHandler),
    ])
    app.listen(3000)
    tornado.ioloop.IOLoop.instance().start()


def post_top():
    while True:
        data = get_top()

        command = {
            "method": "publish",
            "params": {
                "channel": "public",
                "data": data
            }
        }

        api_key = "7bc9937f-542c-493b-9353-254673804ba1"
        data = json.dumps(command)
        headers = {'Content-type': 'application/json', 'Authorization': 'apikey ' + api_key}
        requests.post("http://localhost:8000/api", data=data, headers=headers)


def get_top(top=100):
    while True:
        try:
            conn = psycopg2.connect(dbname='wallets', user='postgres', password='45rtFGvb', host='localhost')
            cursor = conn.cursor()

            break

        except:
            sleep(1)

    cursor.execute('SELECT * FROM wallets ORDER BY balance LIMIT %s', (top,))

    data = {"wallets": []}
    sum_wallet = 0

    for item in cursor:
        wallet = {
            "id": item[0],
            "address": item[1],
            "created_at": item[2].strftime('%Y:%m:%d'),
            "balance": item[3],
            "coin_name": item[4]
        }

        data['wallets'].append(wallet)

        sum_wallet += int(item[3])

    data['sum_of_balance'] = sum_wallet

    cursor.close()
    conn.close()

    return data


if __name__ == '__main__':
    p1 = Process(target=main)
    p1.start()
    p2 = Process(target=post_top)
    p2.start()

