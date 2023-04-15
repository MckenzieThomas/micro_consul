from flask import Flask
from flask import request
from flask import make_response
import sys
import hazelcast
import consul

app = Flask(__name__)


@app.route("/logging_service", methods=['POST'])
def logging_post():
    message_json = request.get_json()
    got_message = message_json['message']
    got_uuid = message_json["uuid"]
    logging_map.put(got_uuid, got_message)
    print("Requested json", sys.stdout)
    print(request.get_json(), sys.stdout)
    response = make_response("Success")
    return response


@app.route("/logging_service", methods=['GET'])
def logging_get():
    return_messages = str(logging_map.values())
    return return_messages


@app.route("/service_check", methods=['GET'])
def facade_check():
    return '', 200


if __name__ == "__main__":
    host = "localhost"
    port = 5001
    consul_service = consul.Consul()
    consul_service.agent.service.register(name="logging_service",
                                          service_id="logging1",
                                          address=host,
                                          port=port,
                                          check={"name": "Checks the logging1 http response",
                                                 f"http": "http://" + host + ":" + str(port) + "/service_check",
                                                 "interval": "10s"})

    client = hazelcast.HazelcastClient()
    index, data = consul_service.kv.get("hazelcast_map")
    haz_map_name = data["Value"].decode()[:-1].replace('"', "")
    logging_map = client.get_map(haz_map_name).blocking()
    app.run(host=host,
            port=port,
            debug=False)
