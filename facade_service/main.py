from flask import Flask
from flask import request
from flask import make_response
import requests
import uuid
import random
import hazelcast
import consul
import sys

app = Flask(__name__)


@app.route("/facade_service", methods=['POST'])
def facade_post():
    message_json = request.get_json()
    send_message = str(message_json['message'])
    random_uuid = uuid.uuid4()
    uuid_str = str(random_uuid)
    send_to_log = {'message': send_message, 'uuid': uuid_str}
    requests.post(url=get_services_address("logging_service"), json=send_to_log)
    response = make_response("Success")
    # add to que
    queue.put(send_to_log)
    return response


@app.route("/facade_service", methods=['GET'])
def facade_get():
    logging_service = requests.get(url=get_services_address("logging_service"))
    messages_service = requests.get(url=get_services_address("messages_service"))
    return_string = "logging_service:" + logging_service.text + "\n messages_service: " + messages_service.text
    return return_string


@app.route("/service_check", methods=['GET'])
def facade_check():
    return '', 200


def get_services_address(service_name):
    index, req = consul_facade.health.service(service_name)
    list_with_services = []
    for element in req:
        address = element.get('Service').get('Address')
        port = element.get('Service').get('Port')
        list_with_services.append(address + ":" + str(port))
    print("List addresses", sys.stdout)
    print(list_with_services, sys.stdout)
    return "http://" + random.choice(list_with_services) + "/" + service_name


if __name__ == "__main__":
    host = "localhost"
    port = 5000
    consul_facade = consul.Consul()
    consul_facade.agent.service.register(name="facade_service",
                                         service_id="facade",
                                         address=host,
                                         port=port,
                                         check={"name": "Checks the facades http response",
                                                f"http": "http://" + host + ":" + str(port) + "/service_check",
                                                "interval": "10s"})
    client = hazelcast.HazelcastClient()
    index, data = consul_facade.kv.get("hazelcast_queue")
    haz_queue_name = data["Value"].decode()[:-1].replace('"', "")
    queue = client.get_queue(haz_queue_name).blocking()
    app.run(host=host,
            port=port,
            debug=False)
