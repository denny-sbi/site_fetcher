import random
import requests
import time
import json
from confluent_kafka import Producer

# string for testing purposes
site_list = '''
{
  "items": [
    {
      "siteId": 64557,
      "siteName": "BH CSG 3 LLC"
    },
    {
      "siteId": 64099,
      "siteName": "Pivot Energy Minnesota Solar 1 LLC"
    },
    {
      "siteId": 64100,
      "siteName": "Pivot Energy Minnesota Solar 10 LLC"
    },
    {
      "siteId": 64253,
      "siteName": "Kankakee Solar 4b LLC"
    },
    {
      "siteId": 64768,
      "siteName": "Pivot Solar 20 LLC"
    },
    {
      "siteId": 64771,
      "siteName": "Pivot Solar 26 LLC"
    },
    {
      "siteId": 64770,
      "siteName": "Pivot Solar 23 LLC"
    },
    {
      "siteId": 64549,
      "siteName": "Pivot Solar 24 LLC"
    },
    {
      "siteId": 64550,
      "siteName": "Pivot Solar 25 LLC"
    },
    {
      "siteId": 64769,
      "siteName": "Pivot Solar 22 LLC"
    },
    {
      "siteId": 64605,
      "siteName": "Pivot Solar 27 LLC"
    },
    {
      "siteId": 64772,
      "siteName": "Pivot Solar 28 LLC"
    },
    {
      "siteId": 64245,
      "siteName": "Pivot Energy PPA 17 LLC"
    },
    {
      "siteId": 64108,
      "siteName": "Pivot Solar NY 9 LLC"
    },
    {
      "siteId": 64101,
      "siteName": "Pivot Solar NY 1 LLC"
    },
    {
      "siteId": 64454,
      "siteName": "Ka Lae Energy, LLC"
    },
    {
      "siteId": 69375,
      "siteName": "Burns Road Solar LLC"
    },
    {
      "siteId": 64523,
      "siteName": "Pivot Solar 21 LLC"
    },
    {
      "siteId": 70998,
      "siteName": "Chaberton Solar Catherine BTM LLC"
    },
    {
      "siteId": 65331,
      "siteName": "Chaberton Solar Catherine ANEM LLC"
    },
    {
      "siteId": 64107,
      "siteName": "Pivot Solar NY 6 LLC"
    },
    {
      "siteId": 64103,
      "siteName": "Pivot Solar NY 3 LLC"
    },
    {
      "siteId": 64249,
      "siteName": "Marion Solar 4 LLC"
    },
    {
      "siteId": 64250,
      "siteName": "Marion Solar 4B LLC"
    },
    {
      "siteId": 65284,
      "siteName": "Chaberton Solar Catherine LLC"
    },
    {
      "siteId": 64251,
      "siteName": "Kankakee Solar 1 LLC"
    },
    {
      "siteId": 64246,
      "siteName": "Grundy Solar 1 LLC"
    },
    {
      "siteId": 64247,
      "siteName": "Putnam Solar 1 LLC"
    },
    {
      "siteId": 64248,
      "siteName": "Vermilion Solar 1b LLC"
    },
    {
      "siteId": 64244,
      "siteName": "Putnam Solar 1b LLC"
    },
    {
      "siteId": 64243,
      "siteName": "St Clair Solar 3 LLC"
    },
    {
      "siteId": 66412,
      "siteName": "Pivot Solar 37 LLC"
    },
    {
      "siteId": 69798,
      "siteName": "Niagara Solar LLC"
    },
    {
      "siteId": 64105,
      "siteName": "Pivot Solar NY 4 LLC"
    },
    {
      "siteId": 67438,
      "siteName": "Pivot Energy PPA 22 LLC"
    },
    {
      "siteId": 64252,
      "siteName": "Kankakee Solar 3 LLC"
    },
    {
      "siteId": 68111,
      "siteName": "Clinton Solar 2b LLC"
    },
    {
      "siteId": 67761,
      "siteName": "Pivot Solar 35 LLC"
    },
    {
      "siteId": 64106,
      "siteName": "Pivot Solar NY 5 LLC"
    },
    {
      "siteId": 68110,
      "siteName": "Clinton Solar 2 LLC"
    },
    {
      "siteId": 68109,
      "siteName": "Marion Solar 2 LLC"
    },
    {
      "siteId": 68173,
      "siteName": "Pivot Solar 30 LLC"
    },
    {
      "siteId": 66982,
      "siteName": "Pivot Energy PPA 27 LLC"
    },
    {
      "siteId": 71309,
      "siteName": "Pivot Energy PPA 28 LLC"
    },
    {
      "siteId": 67782,
      "siteName": "Pivot - Edgar Solar"
    }
  ]
}
'''

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
    '''Fetch site list from also energy, requires session cookie'''
    url = "https://api.alsoenergy.com/Sites?withAlertCounts=false"

    payload = {}
    headers = {
      'accept': 'application/json',
      'Cookie': cookie 
    }

    response = requests.request("GET", url, headers=headers, data=payload)

    if response.status_code == requests.codes.ok:
      print(response.json())
      return response.text
      
    else:
      print('bad response')
      return

def produce_site_list(cookie, producer):
    '''Function to fetch site list and push to kafka topic'''
    try:
        site_data = get_sites(cookie)
    except Exception as e:
        print("error fetching sites!")
        print(e)

        return None

    try:
        # Push data to Kafka topic
        producer.produce('sites', value=site_data, callback=kafka_callback)
        
        # Flush any pending messages to Kafka
        producer.flush()

    except KeyboardInterrupt:
        print("Producer interrupted. Exiting...")

    return site_data

def get_site_info(cookie, site_id):
    '''Fetch site info from also energy, requires session cookie and site id'''
    url = f"https://api.alsoenergy.com/Sites/{site_id}?includeProductionData=false"
    payload = {}

    headers = {
      'accept': 'application/json',
      'Cookie': cookie
    }

    response = requests.request("GET", url, headers=headers, data=payload)

    if response.status_code == requests.codes.ok:
      print(response.json())
      return response.text
      
    else:
      print(f'bad response from site info for site {site_id}')
      return

def get_site_hardware(cookie, site_id):
    '''Fetch site hardware from also energy, requires session cookie and site id'''
    url = f"https://api.alsoenergy.com/Sites/{site_id}/Hardware?includeArchivedFields=false&includeAlertCount=false&includeAlertInfo=false&includeDisabledHardware=false&includeSummaryFields=false&includeDeviceConfig=false&includeDataNameFields=false"

    payload = {}
    headers = {
      'accept': 'application/json',
      'Cookie': cookie
    }

    response = requests.request("GET", url, headers=headers, data=payload)

    if response.status_code == requests.codes.ok:
        response = response.json()
        response['site_id'] = site_id

        return json.dumps(response)
      
    else:
      print(f'bad response from site hardware for site {site_id}')
      return




def produce_site_info(cookie, producer, sites):
    '''Function to fetch site list and push to kafka topic'''

    sites = json.loads(sites)['items']

    for site in sites:
        site_id = site['siteId']

        # info
        try:
            site_data = get_site_info(cookie, site_id)
        except Exception as e:
            print(f"error fetching site info for site {site_id}")
            print(e)
            return

        try:
            # Push data to Kafka topic
            producer.produce('site_info', value=site_data, callback=kafka_callback)
            # Flush any pending messages to Kafka
            producer.flush()

        except KeyboardInterrupt:
            print("Producer interrupted. Exiting...")

        # Hardware
        try:
            site_data = get_site_hardware(cookie, site_id)
        except Exception as e:
            print(f"error fetching site hardware for site {site_id}")
            print(e)
            return

        try:
            # Push data to Kafka topic
            producer.produce('hardware', value=site_data, callback=kafka_callback)
            # Flush any pending messages to Kafka
            producer.flush()

        except KeyboardInterrupt:
            print("Producer interrupted. Exiting...")

    return


def main():
    '''entrypoint'''

    # Read secrets
    with open('/app/secrets.json', 'r') as f:
        secrets = json.load(f)

    email = secrets['email']
    password = secrets['password']

    try:
        cookie = authenticate(email, password)
    except Exception as e:
        print("error generating session cookie!")
        print(e)

    # Set up Kafka producer
    producer_config = {
        'bootstrap.servers': 'kafka:9092',  # containerized networking
        'client.id': 'python-producer'
    }

    producer = Producer(producer_config)



    # Produce sites & keep locally
    sites = produce_site_list(cookie, producer)
    # sites = site_list

    print("Sleeping for 5 seconds to ensure the sites get loaded first")
    time.sleep(5)

    produce_site_info(cookie,producer, sites)  # Use list of sites to produce info


if __name__ == '__main__':
    main()
