import random
import requests
import time
import json
from confluent_kafka import Producer

# Kafka Producer Configuration
producer_config = {
    'bootstrap.servers': 'kafka:9092',  # containerized networking
    'client.id': 'python-producer'
}

def kafka_callback(err, msg):
    ''' Helper function for kafka to callback'''
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic} partition {msg.partition} with offset {msg.offset}")

def authenticate(email, password):
    """
    Authenticate with the AlsoEnergy API and return the session cookie.
    Doesn't require any parameters or inputs
    """
    #logger.debug("Authentication endpoint called")
    # the url for the authentication endpoint
    url = "https://api.alsoenergy.com/Auth/token"
    try:
        # send the email and password in the payload
        payload = {
            'username': email,
            'password': password,
            'grant_type': 'password'
        }
        headers = {
            'accept': 'application/json',
        }
        files = []
        
        response = requests.post(url, headers=headers, data=payload, files=files)
        # check if the response is ok
        if response.status_code == requests.codes.ok:
            #logger.debug("Got Expected Response. Authentication successful ✅")
            # get the access token and token type from the response
            access_token = response.json().get('access_token')
            token_type = response.json().get('token_type')
            # check if the access token is not None
            if access_token is None:
                #logger.error("Access token is null ❌")
                raise HTTPException(status_code=401, detail="Authentication failed")
            # combine the access token and the token type into a cookie
            cookie = f"AlsoEnergyApiSessionCookie={token_type}%20{access_token}"
            return cookie
        else:
            #logger.error("Authentication failed ❌")
            return response.status_code
    except Exception as e:
        #logger.info(f"Error: {e}")
        raise HTTPException(status_code=response.status_code, detail="Internal Server Error")



def get_sites(cookie):
    url = "https://api.alsoenergy.com/Sites?withAlertCounts=false"

    payload = {}
    headers = {
      'accept': 'application/json',
      'Cookie': cookie 
    }

    response = requests.request("GET", url, headers=headers, data=payload)

    if response.status_code == requests.codes.ok:
      # print(response.text)
      payload = response.json()
      print(payload)
      
    else:
      print('bad response')
      return

def main():

    # Read secrets
    with open('/app/secrets.json', 'r') as f:
        secrets = json.load(f)

    email = secrets['email']
    password = secrets['password']

    # Set up Kafka producer
    producer = Producer(producer_config)


    try:
        cookie = authenticate(email, password)
    except Exception as e:
        print("error generating session cookie!")
        print(e)

    try:
        site_data = get_sites(cookie)
    except Exception as e:
        print("error fetching sites!")
        print(e)

    try:
        # Push data to Kafka topic
        producer.produce('sites', value=site_data, callback=kafka_callback)
        
        # Flush any pending messages to Kafka
        producer.flush()

    except KeyboardInterrupt:
        print("Producer interrupted. Exiting...")

if __name__ == '__main__':
    main()
