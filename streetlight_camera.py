import requests
import json
import warnings
import contextlib
import aiohttp
from datetime import datetime
global turnoff_wait
global streetlight_state
from contextlib import suppress
import asyncio
from time import sleep


async def download_url(url, output, apikey):
    """
    :param    url:    url to asynchronously subscribe
    :param output: file to write output
    :param apikey: apikey for accessing the url
    """
    sleep(2)
    skip_initial_data = True
    print('\n*********    Opening TCP: {}     *********\n'.format(url))
    headers = {'apikey': apikey}
    try:
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(verify_ssl=False)) as session:
            async with session.get(url, headers=headers, timeout=300) as response:
                while True:  # loop over for each chunk of data
                    chunk = await response.content.readchunk()
                    if skip_initial_data is True:
                        skip_initial_data = False
                        continue
                    if not chunk:
                        break
                    print(chunk)
                    chunk = chunk[0] # only for linux systems
                    with open("files/{0}".format(output), 'wb') as f:
                        f.write(chunk)
                    with open("files/{0}".format(output), 'r') as f:
                        data = f.read()
                        json_data = json.loads(data)
                        analytics(json_data)
    except Exception as e:
        print("\n*********    Oops: " + url + " " + str(type(e)) + str(e) + "     *********\n")
    print('\n*********    Closing TCP: {}     *********\n'.format(url))


requests.packages.urllib3.disable_warnings()

try:
    from functools import partialmethod
except ImportError:
    # Python 2 fallback: https://gist.github.com/carymrobbins/8940382
    from functools import partial

    class partialmethod(partial):
        def __get__(self, instance, owner):
            if instance is None:
                return self

            return partial(self.func, instance, *(self.args or ()), **(self.keywords or {}))


@contextlib.contextmanager
def no_ssl_verification():
    old_request = requests.Session.request
    requests.Session.request = partialmethod(old_request, verify=False)
    warnings.filterwarnings('ignore', 'Unverified HTTPS request')
    yield
    warnings.resetwarnings()
    requests.Session.request = old_request


def subscribe(api_key, resource_id):
    """ Creates a single tcp connection to middleware and writes its streaming contents asynchronously to a file"""
    try:
        loop = asyncio.get_event_loop()
        i = 0
        while i < 100:
            print("Trying to subscribe :: "+str(i))
            # loop.run_until_complete(download_url("https://10.156.14.144/api/0.1.0/subscribe?name={0}".format(resource_id), "demo_{0}.txt".format(api_key), api_key))
            loop.run_until_complete(
                download_url("http://10.156.14.6:8989/queue?name={0}".format(resource_id),
                             "demo_{0}.txt".format(api_key), api_key))
            i += 1
    except KeyboardInterrupt:  # kills all tasks when SIGTERM is given
        pending = asyncio.Task.all_tasks()
        for task in pending:
            task.cancel()
            with suppress(asyncio.CancelledError):
                loop.run_until_complete(task)



def test(output):
    with open("files/{0}".format(output), 'r') as f:
        data = f.read()
        json_data = json.loads(data)
        analytics(json_data)


def analytics(json):
    global turnoff_wait
    global streetlight_state
    if str(json["count"]) != "0":
        turnoff_wait = 0
        if turnoff_wait == 0 and streetlight_state == "OFF":
            print("{} STREETLIGHT TURNED ON".format(datetime.now()))
            streetlight_state = "ON"
            publish_to_steetlight(100)
        print("{} TURNOFFCOUNT:".format(datetime.now()) + str(turnoff_wait))
    else:
        if turnoff_wait >= 0:
            turnoff_wait += 1
            print("{} TURNOFFCOUNT:".format(datetime.now()) + str(turnoff_wait))
            if turnoff_wait > 20:
                turnoff_wait = -1
                print("{} TURNOFFCOUNT:".format(datetime.now()) + str(turnoff_wait))
                print("{} STREETLIGHT TURNED OFF".format(datetime.now()))
                streetlight_state = "OFF"
                publish_to_steetlight(0)

def publish(api_key, resource_id, data):
    """ Publishes the streetlight data to the middleware.
    Publish URL: https://smartcity.rbccps.org/api/0.1.0/publish

    Args:
        api_key: apikey of the device
        resource_id: device id name
        data: json formatted data to be published
    Returns:
        response: requests response object
    """
    # publish_url = "https://10.156.14.144/api/0.1.0/publish"
    # publish_url = "https://smartcity.rbccps.org/api/0.1.0/publish"
    publish_url = "http://10.156.14.6:8989/publish"
    publish_headers = {"apikey": api_key}

    publish_data = {"exchange": "amq.topic",
                    "key": resource_id,
                    "body": str(json.dumps(data))}

    print("\n*********    Publisher: Sending data to Middleware    *********\n")
    print(publish_headers)
    print(publish_data)
    with no_ssl_verification():
        r = requests.post(publish_url, json.dumps(publish_data), headers=publish_headers)
    return r


def publish_to_steetlight(brightness):
    if int(brightness) < 0:
        brightness = 0
    if int(brightness) > 100:
        brightness = 100
    command = dict()
    command["ManualControlParams"] = {}
    command["ManualControlParams"]["targetBrightnessLevel"] = int(brightness)
    api_key = "beee69bb9d024fbf97800be726f85a57"
    publish(api_key, "70b3d58ff0031f03_update", command)


if __name__ == '__main__':
    global turnoff_wait
    global streetlight_state
    streetlight_state = "OFF"
    turnoff_wait = 0
    subscribe('8bf443fcfc18470f81b28216142d413c', 'test100')
    publish_to_steetlight(0)
