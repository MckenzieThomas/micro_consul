import sys
import threading
from flask import Flask
from flask import make_response
import hazelcast
import consul

app = Flask(__name__)


@app.route("/messages_service", methods=['GET'])
def messages_get():
    return_messages = str(list(dict.values()))
    return return_messages


@app.route("/service_check", methods=['GET'])
def facade_check():
    return '', 200


def def_thread():
    while True:
        if not queue.is_empty():
            head = queue.take()
            dict.update({head['uuid']: head['message']})
            print(str(head), sys.stdout)
            print(str(dict), sys.stdout)


if __name__ == "__main__":
    host = "localhost"
    port = 5005
    consul_messages = consul.Consul()
    consul_messages.agent.service.register(name="messages_service",
                                           service_id="messages1",
                                           address=host,
                                           port=port,
                                           check={"name": "Checks the messages1 http response",
                                                  f"http": "http://" + host + ":" + str(port) + "/service_check",
                                                  "interval": "10s"})
    dict = {}
    client = hazelcast.HazelcastClient()
    index, data = consul_messages.kv.get("hazelcast_queue")
    haz_queue_name = data["Value"].decode()[:-1].replace('"', "")
    queue = client.get_queue(haz_queue_name).blocking()
    consumer_thread = threading.Thread(target=def_thread)
    consumer_thread.start()
    lock_thr = threading.RLock()
    app.run(host=host,
            port=port,
            debug=False)
