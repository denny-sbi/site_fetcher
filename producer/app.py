import random
import requests
import time
from datetime import datetime, timedelta
import json
from confluent_kafka import Producer

class SiteFetcher:
    def __init__(self):
        # Custom chart IDs for AlsoEnergy
        self.CUSTOM_CHART_IDS = {
            "Production meter net energy": "Energy",
            "Inverter net energy": "Energy",
            "POA Sensor":              "Irradiance",
            "POA":                     "Irradiance",
            "Insolation estimate":     "Irradiance",
            "Expected energy":         "ExpectedEnergy",
            "Estimated Production":    "EstimatedEnergy",
            "Weighted Site Uptime":    "Uptime",
            "Site availability":       "Availability",
            "Clipping loss":           "Clipping",
            "Snow loss":               "Snow",
            "Inverter loss":           "Inverter",
            "Downtime loss":           "Downtime",
            "Estimated Downtime loss": "EstimatedDowntime"
        }

        # Mapping of the custom chart IDs to the hardware types
        self.METRIC_HARDWARE_MAP = {
            "Production meter net energy": ["meters"],
            "Inverter net energy":    ["inverters"],
            "POA Sensor":             ["pyranometers", "weather_stations"],
            "POA":                    ["pyranometers", "weather_stations"],
            "Insolation estimate":    ["pyranometers", "weather_stations"],
            "Expected energy":        ["pyranometers", "weather_stations", "meters"],
            "Estimated Production":   ["meters"],
            "Weighted Site Uptime":   ["pyranometers", "weather_stations", "inverters"],
            "Site availability":      ["pyranometers", "weather_stations", "inverters", "meters"],
            "Clipping loss":          ["pyranometers", "weather_stations", "meters"],
            "Snow loss":              ["pyranometers", "weather_stations", "inverters", "meters"],
            "Inverter loss":          ["pyranometers", "weather_stations", "inverters", "meters"],
            "Downtime loss":          ["pyranometers", "weather_stations", "inverters", "meters"],
            "Estimated Downtime loss":["pyranometers", "weather_stations", "inverters", "meters"]
        }

        self.SELECTED_OPTIONS_BY_METRIC = {
            "POA":                 "UseInsolation,POA",
            "Insolation estimate": "IncludePOA,InsolationEst",
            "Estimated Production": "Monthly straight line",
            "POA Sensor":          "IncludePOA,Sensor",
        }

        self.read_credentials()

        try:
            self.cookie = self.authenticate()
        except Exception as e:
            print("error generating session cookie!")
            print(e)

        # Set up Kafka producer
        producer_config = {
            'bootstrap.servers': 'kafka:9092',  # containerized networking
            'client.id': 'python-producer'
        }

        self.producer = Producer(producer_config)


    def read_credentials(self):
        # Read secrets
        with open('/app/secrets.json', 'r') as f:
            secrets = json.load(f)

        self.email = secrets['email']
        self.password = secrets['password']


    def kafka_callback(self, err, msg):
        ''' Helper function for kafka to callback'''
        if err is not None:
            print(f"Message delivery failed: {err}")
        else:
            print(f"Message delivered to {msg.topic} partition {msg.partition} with offset {msg.offset}")

    def authenticate(self):
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
                'username': self.email,
                'password': self.password,
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
                    print("access token is Null")
                    raise Exception()
                # combine the access token and the token type into a cookie
                cookie = f"AlsoEnergyApiSessionCookie={token_type}%20{access_token}"
                return cookie
            else:
                #logger.error("Authentication failed ❌")
                return response.status_code
        except Exception as e:
            #logger.info(f"Error: {e}")
            raise Exception()



    def get_sites(self):
        '''Fetch site list from also energy, requires session cookie'''
        url = "https://api.alsoenergy.com/Sites?withAlertCounts=false"

        headers = {
            'accept': 'application/json',
            'Cookie': self.cookie 
        }

        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
        except requests.RequestException as e:
            print(f"[get_sites] error fetching sites: {e}")
            return None

        try:
            payload = response.json()
            sites = payload.get('items', [])
            print(f"[get_sites] fetched {len(sites)} sites from API")
            return response.text
        except ValueError as e:
            print(f"[get_sites] invalid JSON response: {e}")
            return None



    def get_site_info(self, cookie, site_id):
        '''Fetch site info from also energy, requires session cookie and site id'''
        url = f"https://api.alsoenergy.com/Sites/{site_id}?includeProductionData=false"
        payload = {}

        headers = {
            'accept': 'application/json',
            'Cookie': cookie
        }

        response = requests.request("GET", url, headers=headers, data=payload)

        if response.status_code == requests.codes.ok:
            return response.text
            
        else:
            print(f'bad response from site info for site {site_id}')
            return

    def produce_site_info(self, site_id, site_name):
            try:
                site_data = self.get_site_info(self.cookie, site_id)
            except Exception as e:
                print(f"error fetching site info for site {site_id} {site_name}")
                print(e)
                return

            try:
                # Push data to Kafka topic
                self.producer.produce('sites', value=site_data, callback=self.kafka_callback)
                # Flush any pending messages to Kafka
                self.producer.flush()
            except KeyboardInterrupt:
                print("Producer interrupted. Exiting...")

    def get_site_hardware(self, site_id, site_name):
        '''Fetch site hardware from also energy, requires session cookie and site id'''
        url = f"https://api.alsoenergy.com/Sites/{site_id}/Hardware?includeArchivedFields=false&includeAlertCount=false&includeAlertInfo=false&includeDisabledHardware=false&includeSummaryFields=false&includeDeviceConfig=false&includeDataNameFields=false"

        payload = {}
        headers = {
            'accept': 'application/json',
            'Cookie': self.cookie
        }

        response = requests.request("GET", url, headers=headers, data=payload)

        if response.status_code == requests.codes.ok:
            response = response.json()
            response['site_id'] = site_id
            response['site_name'] = site_name

            return json.dumps(response)
        
        else:
            print(f'bad response from site hardware for site {site_id}')
            return

    def produce_hardware(self, hardwares):
        if not hardwares:
            return
        try:
            # Push data to Kafka topic
            self.producer.produce('hardware', value=hardwares, callback=self.kafka_callback)
            # Flush any pending messages to Kafka
            self.producer.flush()
        except KeyboardInterrupt:
            print("Producer interrupted. Exiting...")


    def produce_site_list(self, sites):
        '''Function to fetch site list and push to kafka topic'''
        try:
            # Push data to Kafka topic
            self.producer.produce('sites', value=sites, callback=self.kafka_callback)
            
            # Flush any pending messages to Kafka
            self.producer.flush()

            print("Sites data pushed to kafka")
        except KeyboardInterrupt:
            print("Producer interrupted. Exiting...")
            
        return

    def get_types(self, hardwares):
        '''Fetch site hardware in categories from also energy, requires session cookie and site id'''
        hardwares = json.loads(hardwares)
        categories = {
            "meters": [],
            "inverters": [],
            "cellular_modems": [],
            "data_loggers": [],
            "weather_stations": [],
            "pyranometers": [],
            "tracker_controllers": [],
            "ups": [],
            "uncategorized": []
        }
            
        for equipment in hardwares['hardware']:
            fc = equipment['functionCode']
            name = equipment['name'].lower()

            if fc == 'PM':
                categories["meters"].append(equipment)
            elif fc == 'PV':
                categories["inverters"].append(equipment)
            elif fc == 'DA' and 'cellular' in name:
                categories["cellular_modems"].append(equipment)
            elif fc == 'GW':
                categories["data_loggers"].append(equipment)
            elif fc == 'WS' and 'pyranometer' in name:
                categories["pyranometers"].append(equipment)
            elif fc == 'WS':
                categories["weather_stations"].append(equipment)
            elif fc == 'ST':
                categories["tracker_controllers"].append(equipment)
            elif fc == 'DA' and 'ups' in name:
                categories["ups"].append(equipment)
            else:
                categories["uncategorized"].append(equipment)

        return categories

    def get_hardware_metrics(self, metric_key, site_id, site_name, hw_ids, start_date, end_date):
        '''Fetch custom metric data from also energy, requires metric key, site_id, hardware, start_date, end_date, and cookie'''
        chart_id = self.CUSTOM_CHART_IDS.get(metric_key)
        if not chart_id:
            raise ValueError(f"Invalid metric key: {metric_key}")
        url = f"https://api.alsoenergy.com/Charts/Custom/{chart_id}/Data"

        headers = {
            'accept': 'application/json',
            'Cookie': self.cookie
        }

        params = {
            "startTime": f"{start_date}T00:00:00",
            "endTime": f"{end_date}T00:00:00",
            "span": "Custom",
            "binSize": "Bin15Min",
            "aggregationMode": "BySite",
            "hardwareIds": str(hw_ids),
            "lineType": "Line",
            "selectedOptions": self.SELECTED_OPTIONS_BY_METRIC.get(metric_key)
        }
        
        response = requests.request("GET", url, headers=headers, params=params)
        if response.status_code == requests.codes.ok:
            data = response.json()
            data["site_id"] = site_id
            data["site_name"] = site_name
            data["hardware_ids"] = hw_ids
            data["metric"] = metric_key
            return json.dumps(data)
        else:
            print(f"Error fetching custom metric data for {metric_key} from {site_id} {site_name} / {hw_ids} : {response.status_code}") 
            return None

    def produce_hardware_metrics(self, metric, site, hardwares, start_date, end_date):
        '''Function to fetch metrics and push to kafka topic'''

        # Format the dates in the required format (e.g., 2025-03-01T00%3A00%3A00)
        from_date = start_date.strftime("%Y-%m-%d")
        to_date = end_date.strftime("%Y-%m-%d")

        site_id = site['siteId']
        site_name = site['siteName']


        if not hardwares:
            print(f"[{metric}] No hardware retrieved for site {site_id} {site_name}")
            # logger.log_site(site_id, site_name, metric, [], "fail", f"No hardware retrieved for metric: {metric}")
            return
        else:
            hardware_by_type = self.get_types(hardwares)
            categories = self.METRIC_HARDWARE_MAP.get(metric, [])
            hardware_list = []
            for cat in categories:
                hardware_list.extend(hardware_by_type.get(cat, []))
            
            hw_ids = [hw["id"] for hw in hardware_list]
            hw_ids_str = ",".join(map(str, hw_ids))
            try:
                chart_data = self.get_hardware_metrics(metric, site_id, site_name, hw_ids_str, from_date, to_date)
                if chart_data:
                    self.producer.produce('hardware_metrics', value=chart_data, callback=self.kafka_callback)
                    self.producer.flush()
                    print(f"Produced custom chart data for {metric} from site {site_id} {site_name}, hardware {hw_ids}")
                    # logger.log_site(site_id, site_name, metric, [], "success")
                else:
                    print(f"Received empty response for {metric} from site {site_id} {site_name}, hardware {hw_ids}")
                    # logger.log_site(site_id, site_name, metric, hw_ids, "null", f"Empty response for metric {metric}")	
            except Exception as e:
                print(f"error fetching custom metric data for {metric} for site {site_id} {site_name}, hardware {hw_ids}: {e}")
                # logger.log_site(site_id, site_name, metric, hw_ids, "fail", f"{metric} error: {e}")
                return
        return

    def get_data(self, start_date=None, end_date=None):
        ''' Function to handle logic of grabbing data from AlsoEnergy
        
            TODO: Backfill functionality
        '''

        # TODO backfill logic
        start_date = datetime.now() - timedelta(days=1) # midnight yesterday
        end_date = datetime.now() # midnight today


        sites_raw = self.get_sites()  # API call to grab list of sites
        # self.produce_site_list(sites_raw)  # Push site list to kafka feed
        sites = json.loads(sites_raw)['items']

        metrics = list(self.METRIC_HARDWARE_MAP.keys())  # List of metrics we are collecting
        # logger = LogWriter()
        
        for site in sites:
            site_id = site['siteId']
            site_name = site['siteName']

            # Get hardware for that site
            try:
                hardwares = self.get_site_hardware(site_id, site_name)
            except Exception as e:
                print(f"error fetching site hardware for site {site_id} {site_name}")
                print(e)
                # logger.log_site(site_id, site_name, "all", [], "fail", f"Hardware fetch error: {e}")
                continue

            self.produce_site_info(site_id, site_name)  # Push information for site
            self.produce_hardware(hardwares)  # Push hardware associated with site

            # Loop through every metric and produce for that site
            for metric in metrics:
                self.produce_hardware_metrics(metric, site, hardwares, start_date, end_date)

                # Write the log to a sheet
                # logger.write_sheet(start_date, end_date)
	

def main():
    '''entrypoint'''

    fetcher = SiteFetcher()  # Iniitialize object

    # TODO take date range
    fetcher.get_data()  # Perform data extraction


if __name__ == '__main__':
    main()
