import requests
import uuid
import json
from datetime import datetime, timedelta
from confluent_kafka import Producer
from solarbi import SolarBIComms
from utils import kafka_callback, get_retrying_session
from time import time
from active_sites import active_sites

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# CONSTANTS:
TODAY = datetime.now().date()
YESTERDAY = TODAY - timedelta(days=1)
RUN_DATE = datetime.now()


class SiteFetcher:
    def __init__(self, use_kafka=True, start_date=None, end_date=None):
        '''Initialize the SiteFetcher object, setting up authentication and Kafka producer if needed'''

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

        self.RUN_ID = str(uuid.uuid4())
        self.run_date = RUN_DATE

        self.start_date, self.end_date, self.process = self.initialize_dates(start_date, end_date)
        logger.info(f"Initialized SiteFetcher with start date {self.start_date} {type(self.start_date)} and end date {self.end_date} {type(self.end_date)} with process type {self.process}")

        self.read_credentials()

        self.session = get_retrying_session()

        try:
            self.cookie = self.authenticate()
        except Exception as e:
            print("error generating session cookie!")
            print(e)

        self.use_kafka = use_kafka
        if self.use_kafka:
            print("Kafka producer enabled, initializing producer...")

            # Set up Kafka producer
            producer_config = {
                'bootstrap.servers': 'kafka:9092',  # containerized networking
                'client.id': 'python-producer'
            }

            self.producer = Producer(producer_config)

            self.comms = SolarBIComms(self.producer, self.RUN_ID)

            run_info = json.dumps({
                'timestamp': str(datetime.now()),
                'runID': self.RUN_ID,
                'process': self.process,
                'event': 'start'

            })

            self.producer.produce('solarbi_runs', value=run_info, callback=kafka_callback)  # Push Run start to Kafka
            self.producer.flush()
        else:
            print("Kafka producer disabled, running without Kafka...")
            self.producer = None
            self.comms = None


    def initialize_dates(self, start_date, end_date):
        '''Initialize start and end dates for data fetching'''

        # check if start date and end date are provided
        # check if the dates are in the correct format
        # check if the start date is before the end date
        logger.info(f"Provided dates Start date: {start_date}, End date: {end_date}")
        if start_date is None or end_date is None:
            return YESTERDAY, TODAY, 'daily'
        
        try:
            start_date = datetime.strptime(start_date, "%Y-%m-%d")
        except ValueError:
                raise ValueError(f"Start date is required, but got {start_date}. Format should be YYYY-MM-DD")
        try:
            end_date = datetime.strptime(end_date, "%Y-%m-%d")
        except ValueError:
                raise ValueError(f"End date is required, but got {end_date}. Format should be YYYY-MM-DD")

        if start_date >= end_date:
            raise ValueError(f"Start date {start_date} is greater than end date {end_date}")
        else:
            return start_date, end_date, 'backfill'


    
    def read_credentials(self):
        # Read secrets
        
        # check if the app/secrets.json file exists and read the email and password from it
        # otherwise, look in ../secrets.json for the file and read from there
        # otherwise, raise an exception that the secrets file could not be found
        secrets = {}
        print("Reading secrets from /app/secrets.json...")
        
        print("If this fails, ensure that the secrets file is mounted correctly in the container.")
        print("If running locally, ensure that the secrets file is in the correct location.")
        print("If running in production, ensure that the secrets file is mounted correctly in the container and that the path is correct.")
        try:
            with open('/app/secrets.json', 'r') as f:
                secrets = json.load(f)
        except Exception as e:
            print("Error reading secrets file: ", e)
            print("Attempting to read secrets file from ../secrets.json...")
            try:
                with open('./secrets.json', 'r') as f:
                    secrets = json.load(f)
            except Exception as e:
                print("Error reading secrets file: ", e)
                raise FileNotFoundError("Secrets file not found")

        self.email = secrets['email']
        self.password = secrets['password']


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
            
            response = self.session.post(url, headers=headers, data=payload, files=files)
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
            response = self.session.request("GET", url, headers=headers)
            response.raise_for_status()
        except requests.RequestException as e:
            print(f"[get_sites] error fetching sites: {e}")
            if self.comms:
                self.comms.record_comms_event('Site List', -1, 'Site List', f'HTTP Error - {response.status_code}')
            return None

        try:
            payload = response.json()
            sites = payload.get('items', [])
            print(f"[get_sites] fetched {len(sites)} sites from API")
            return response.text
        except ValueError as e:
            print(f"[get_sites] invalid JSON response: {e}")
            if self.comms:
                self.comms.record_comms_event('Site List', -1, 'Site List', 'Parsing Error')

            return None



    def get_site_info(self, site_id):
        '''Fetch site info from also energy, requires session cookie and site id'''
        url = f"https://api.alsoenergy.com/Sites/{site_id}?includeProductionData=false"
        payload = {}

        headers = {
            'accept': 'application/json',
            'Cookie': self.cookie
        }

        response = self.session.request("GET", url, headers=headers, data=payload)

        if response.status_code == requests.codes.ok:
            return response.text
            
        else:
            print(f'bad response from site info for site {site_id}')
            if self.comms:
                self.comms.record_comms_event('Site', site_id, 'Site Info', f'HTTP Error - {response.status_code}')
            return

    def produce_sites(self, site_id, site_name):
            try:
                site_data = self.get_site_info(site_id)
            except Exception as e:
                print(f"error fetching site info for site {site_id} {site_name}")
                if self.comms:
                    self.comms.record_comms_event('Site', site_id, 'Site Info', 'Parsing Error')

                print(e)
                return

            try:
                if self.producer:    
                    # Push data to Kafka topic
                    self.producer.produce('sites', value=site_data, callback=kafka_callback)
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

        response = self.session.request("GET", url, headers=headers, data=payload)

        if response.status_code == requests.codes.ok:
            response = response.json()
            response['site_id'] = site_id
            response['site_name'] = site_name
            response['recordDate'] = str(datetime.now())
            return json.dumps(response)
        
        else:
            print(f'bad response from site hardware for site {site_id}')
            if self.comms:
                self.comms.record_comms_event('Site', site_id, 'Site Hardware', f'HTTP Error - {response.status_code}')

            return

    def produce_hardware(self, hardwares):
        if not hardwares:
            return
        try:
            if self.producer:
                # Push data to Kafka topic
                self.producer.produce('hardware', value=hardwares, callback=kafka_callback)
                # Flush any pending messages to Kafka
                self.producer.flush()
        except KeyboardInterrupt:
            print("Producer interrupted. Exiting...")


    def produce_site_list(self, sites):
        '''Function to fetch site list and push to kafka topic'''
        try:
            if self.producer:
                # Push data to Kafka topic
                self.producer.produce('sites_list', value=sites, callback=kafka_callback)
                
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
        
        # check that start_date and end_date are in the correct format
        assert isinstance(start_date, str) and isinstance(end_date, str), f"start_date and end_date must be strings in YYYY-MM-DD format, they were {start_date} and {end_date} of types {type(start_date)} and {type(end_date)}"
        assert len(start_date) == 10 and len(end_date) == 10, f"start_date and end_date must be in YYYY-MM-DD format, they were {start_date} and {end_date} of lengths {len(start_date)} and {len(end_date)}"
        assert start_date[4] == '-' and start_date[7] == '-', f"start_date must be in YYYY-MM-DD format, it was {start_date}"
        assert end_date[4] == '-' and end_date[7] == '-', f"end_date must be in YYYY-MM-DD format, it was {end_date}"
        assert start_date <= end_date, f"start_date must be before or equal to end_date, they were {start_date} and {end_date}"

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

        # print(f"params: {params}")
        start = time()
        response = self.session.request("GET", url, headers=headers, params=params)
        if response.status_code == requests.codes.ok:
            data = response.json()
            data["site_id"] = site_id
            data["site_name"] = site_name
            data["hardware_ids"] = hw_ids
            data["metric"] = metric_key
            self.comms.record_comms_event('Hardware Metrics', hw_ids[0], metric_key, 'Successful Insertion')
            print(f"get_hardware_metrics for {metric_key} from {site_id} {site_name} / {hw_ids} took {time() - start:.2f} seconds")
            return json.dumps(data)
        elif response.status_code != 204:
            print(f"Error fetching custom metric data for {metric_key} from {site_id} {site_name} / {hw_ids} : {response.status_code}") 
            if self.comms:
                self.comms.record_comms_event('Site', site_id, metric_key, f'HTTP Error - {response.status_code}')

            for hw_id in hw_ids.split(","):

                if hw_id is not None and hw_id != '':
                    if self.comms:
                        self.comms.record_comms_event('Hardware', hw_id, metric_key, f'HTTP Error - {response.status_code}')
            return None

    def produce_hardware_metrics(self, metric, site, hardwares, start_date, end_date):
        '''Function to fetch metrics and push to kafka topic'''



        site_id = site['siteId']
        site_name = site['siteName']


        if not hardwares:
            print(f"[{metric}] No hardware retrieved for site {site_id} {site_name}")
            return
        else:
            hardware_by_type = self.get_types(hardwares)
            categories = self.METRIC_HARDWARE_MAP.get(metric, [])
            hardware_list = []
            for cat in categories:
                hardware_list.extend(hardware_by_type.get(cat, []))
            
            hw_ids = [hw["id"] for hw in hardware_list]

            try:
                # For certain metrics, we need to get the production for each indvidual hardware ID
                # This avoids AlsoEnergy API summing it for us
                hw_groups = []
                if metric in ("Production meter net energy", "Inverter net energy", "Estimated Production"):
                    hw_groups = [[x] for x in hw_ids]  # Separate API calls
                else:
                    hw_groups = [hw_ids]

                # Form a call for each group necessary 
                start = time()
                for hw_ids in hw_groups:
                    hw_ids_str = ",".join([str(x) for x in hw_ids])
                    chart_data = self.get_hardware_metrics(
                        metric, 
                        site_id, 
                        site_name, 
                        hw_ids_str, 
                        start_date.strftime("%Y-%m-%d"), 
                        end_date.strftime("%Y-%m-%d")
                        )
                    if chart_data:
                        self.producer.produce('hardware_metrics', value=chart_data, callback=kafka_callback)
                        self.producer.flush()

                        # if self.comms:
                        #     self.comms.record_comms_event('Hardware Metric', hw_ids_str[0], metric, f'Successful insertion')

                        print(f"Produced custom chart data for {metric} from site {site_id} {site_name}, hardware {hw_ids} in {time() - start:.2f} seconds")
                        # logger.log_site(site_id, site_name, metric, [], "success")
                    else:
                        print(f"Received empty response for {metric} from site {site_id} {site_name}, hardware {hw_ids}")
                        # logger.log_site(site_id, site_name, metric, hw_ids, "null", f"Empty response for metric {metric}")	
            except Exception as e:
                print(f"error fetching custom metric data for {metric} for site {site_id} {site_name}, hardware {hw_ids}: {e}")
                # logger.log_site(site_id, site_name, metric, hw_ids, "fail", f"{metric} error: {e}")
                self.comms.record_comms_event('Hardware Metric', hw_ids_str[0], metric, f'{e}')

                return
        return

    def get_data(self):
        ''' Function to handle logic of grabbing data from AlsoEnergy
        
            Args:
                start_date (str): Start date in YYYY-MM-DD format (default: yesterday)
                end_date (str): End date in YYYY-MM-DD format (default: today)
        '''

        print(f'Starting data fetch for process type: {self.process} from {self.start_date} to {self.end_date}')

        try:
            start_date = self.start_date
            end_date = self.end_date
            
            print('getting sites')
            sites_raw = self.get_sites()  # API call to grab list of sites
            self.produce_site_list(sites_raw)  # Push site list to kafka feed
            sites = json.loads(sites_raw)['items']

            metrics = list(self.METRIC_HARDWARE_MAP.keys())  # List of metrics we are collecting
            
            print(f'site list is length {len(sites)} of type {type(sites)}')
            print(f'{sites=}')
            for site in sites:
                site_id = site['siteId']
                site_name = site['siteName']

                if active_sites and site_id not in active_sites:
                    print(f"{site_id} not in active sites list. Skipping inactive site {site_id} {site_name}.")
                    continue

                print(f'Processing site {site_id} {site_name} with process type {self.process} at {datetime.now()}')

                # Get hardware for that site
                try:
                    start = time()
                    hardwares = self.get_site_hardware(site_id, site_name)
                    print(f"get_site_hardware took {time() - start:.2f} seconds")
                except Exception as e:
                    print(f"error fetching site hardware for site {site_id} {site_name}")
                    print(e)
                    continue

                start = time()
                self.produce_sites(site_id, site_name)  # Push information for site
                self.comms.record_comms_event('Site', site_id, 'Site Info', f'Successful insertion')
                print(f"produce_sites took {time() - start:.2f} seconds")

                start = time()
                self.produce_hardware(hardwares)  # Push hardware associated with site
                self.comms.record_comms_event('Site', site_id, 'Hardware Associations', f'Successful insertion')
                print(f"produce_hardware took {time() - start:.2f} seconds")

                # Loop through every metric and produce for that site
                for metric in metrics:
                    if self.process == 'backfill':
                        backfill_start = time()
                        print(f'starting backfill for site {site_name} metric {metric} from {start_date} to {end_date}')
                        date = self.start_date
                        while date < self.end_date:
                            start = time()
                            batch_next_date = min(date + timedelta(days=5), self.end_date)
                            # next_date = date + timedelta(days=1)
                            self.produce_hardware_metrics(
                                metric, 
                                site, 
                                hardwares, 
                                date, #.strftime("%Y-%m-%d"),  #start date must be a string in YYYY-MM-DD format
                                batch_next_date #.strftime("%Y-%m-%d") #end date must be a string in YYYY-MM-DD format
                                )
                            print(f'produced {date.strftime("%Y-%m-%d")} for metric {metric} and hardwares in {time() - start:.2f} seconds')
                            date = batch_next_date
                        self.comms.record_comms_event('Site', site_id, metric, f'Successful backfill')
                        # self.comms.record_comms_event('Hardware Metrics', entity_id=, metric, f'Backfill completed from {self.start_date} to {self.end_date}')
                        print(f"completed backfill for site {site_name} metric {metric} from {start_date} to {end_date}, took {time() - backfill_start:.2f} seconds")
                    else:
                        self.produce_hardware_metrics(metric, site, hardwares, start_date, end_date)
                        self.comms.record_comms_event('Site', site_id, metric, f'Successful daily')
                        
                

        except Exception as e:
            print(e)
            run_info = json.dumps({
                'timestamp': str(datetime.now()),
                'runID': self.RUN_ID,
                'process': self.process,
                'event': 'failed'
            })
            self.producer.produce('solarbi_runs', value=run_info, callback=kafka_callback)  # Push Run failure to Kafka
            self.producer.flush()
            # exit()
            
          
        # Done
        run_info = json.dumps({
            'timestamp': str(datetime.now()),
            'runID': self.RUN_ID,
            'process': self.process,
            'event': 'finished'

        })

        print(f'finished process with {json.loads(run_info)}')

        self.producer.produce('solarbi_runs', value=run_info, callback=kafka_callback)  # Push Run end to Kafka
        self.producer.flush()
        return


	

def main():
    '''entrypoint'''

    # suspect that backfill cannot work for longer than 5 days at a time
    # tested backfill on 10/14 and running in chunks of 2,4, and 9 days works. It ran from 09-01 to 09-22

    fetcher = SiteFetcher()  # Iniitialize object
    fetcher.get_data()  # Perform data extraction
    # fetcher = SiteFetcher(start_date='2025-09-22',end_date='2025-09-27')  # Iniitialize object

    # # TODO take date range
    # fetcher.get_data()  # Perform data extraction

    # fetcher = SiteFetcher(start_date='2025-09-28',end_date='2025-10-01')  # Iniitialize object

    # # TODO take date range
    # fetcher.get_data()  # Perform data extraction






if __name__ == '__main__':
    main()