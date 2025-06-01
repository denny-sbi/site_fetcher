import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

def get_retrying_session(
        retries=5,
        backoff_factor=1,
        status_forcelist=(429, 500, 502, 503, 504),
        session=None
    ):

    session = session or requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        status=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        allowed_methods=["HEAD", "GET", "OPTIONS", "POST", "PUT", "DELETE"],
        raise_on_status=False,
    )

    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

def kafka_callback(err, msg):
    ''' Helper function for kafka to callback'''
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic} partition {msg.partition} with offset {msg.offset}")


