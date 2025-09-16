import requests
import uuid
import json
import argparse
from datetime import datetime, timedelta
from confluent_kafka import Producer
from solarbi import SolarBIComms
from utils import kafka_callback, get_retrying_session

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# CONSTANTS:
TODAY = datetime.now().date()
YESTERDAY = TODAY - timedelta(days=1)
RUN_DATE = str(datetime.now())

# TODO: add a start and end date to the constructor. the start and end date get added to self.start_date and self.end_date. Additionally, they are checked for the correct format.
# if the format is incorrect, raise an exception
# TODO: end_date inclusive or exclusive? end_date is inclusive BUT it counts as midnight of the end date, so that day is not fetched, which makes it FEEL exclusive.
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

        self.start_date = start_date
        self.end_date = end_date
        self.run_date = RUN_DATE

        # daily processing is default, when no arguments for dates are provided
        if self.start_date is None and self.end_date is None:
            self.start_date = YESTERDAY
            self.end_date = TODAY
        # process the start date if provided by user
        elif self.start_date is None:
            try:
                self.start_date = datetime.strptime(self.start_date, "%Y-%m-%d")
            except ValueError:
                raise ValueError(f"Start date is required, but got {self.start_date}. Format should be YYYY-MM-DD")
        # process the end date if provided by user
        elif self.end_date is None:
            try:
                self.end_date = datetime.strptime(self.end_date, "%Y-%m-%d")
            except ValueError:
                raise ValueError(f"End date is required, but got {self.end_date}. Format should be YYYY-MM-DD")
        
        # check if the start date is greater than the end date
        if self.start_date > self.end_date:
            raise ValueError(f"Start date {self.start_date} is greater than end date {self.end_date}")

        # process should be daily if start date and end date are 1 day apart. If start date is more than 2 days ago, than it should be backfill.
        self.process = 'daily' 
        if start_date != YESTERDAY:
            self.process = 'backfill'
        
        

        self.read_credentials()

        self.session = get_retrying_session()

        try:
            self.cookie = self.authenticate()
        except Exception as e:
            print("error generating session cookie!")
            print(e)

        # Set up Kafka producer
        self.use_kafka = use_kafka

        if self.use_kafka:
            print("Kafka producer enabled, initializing producer...")
            producer_config = {
                'bootstrap.servers': 'kafka:9092',  # containerized networking
                'client.id': 'python-producer'
            }

            self.producer = Producer(producer_config)
            
            self.comms = SolarBIComms(self.producer, self.RUN_ID)

            run_info = json.dumps({
                'timestamp': start_date,
                'runID': self.RUN_ID,
                'process': self.process,
                'event': 'start',
                'run_date':self.run_date,

            })

            self.producer.produce('solarbi_runs', value=run_info, callback=kafka_callback)  # Push Run start to Kafka
            self.producer.flush()
        
        else:
            print("Kafka producer disabled, running without Kafka...")
            self.producer = None
            self.comms = None

            


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
            print(f"Authentication response status code: {response.status_code}, headers: {headers}, payload: {payload} ")
            # check if the response is ok
            if response.status_code == requests.codes.ok:
                logger.debug("Got Expected Response. Authentication successful ✅")
                # get the access token and token type from the response
                access_token = response.json().get('access_token')
                token_type = response.json().get('token_type')
                # check if the access token is not None
                if access_token is None:
                    logger.error("Access token is null ❌")
                    print("access token is Null")
                    raise Exception()
                # combine the access token and the token type into a cookie
                cookie = f"AlsoEnergyApiSessionCookie={token_type}%20{access_token}"
                return cookie
            else:
                logger.error("Authentication failed ❌")
                return response.status_code
        except Exception as e:
            logger.info(f"Error: {e}")
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
                self.comms.record_comms_event(entity_type='Site List', entity_id=-1, metric='Site List', event=f'HTTP Error - {response.status_code}', process=self.process, start_date=self.start_date, run_date=self.run_date)

            return None

        try:
            payload = response.json()
            sites = payload.get('items', [])
            print(f"[get_sites] fetched {len(sites)} sites from API")
            return response.text
        except ValueError as e:
            print(f"[get_sites] invalid JSON response: {e}")
            if self.comms:
                self.comms.record_comms_event(entity_type='Site List', entity_id=-1, metric='Site List', event=f'Parsing Error', process=self.process, start_date=self.start_date, run_date=self.run_date)


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

            self.comms.record_comms_event(entity_type='Site', entity_id=site_id, metric='Site Info', event=f'HTTP Error - {response.status_code}', process=self.process, start_date=self.start_date, run_date=self.run_date)
            return

    def produce_sites(self, site_id, site_name):
            try:
                site_data_raw = self.get_site_info(site_id)
                if site_data_raw is None:
                    logging.info(f"site data raw is None for site {site_id} {site_name}")
                    return
                site_data = json.loads(site_data_raw)
            except Exception as e:
                print(f"error fetching site info for site {site_id} {site_name}")
                if self.comms:
                   self.comms.record_comms_event(entity_type='Site', entity_id=site_id, metric='Site Info', event='Parsing Error', process=self.process, start_date=self.start_date, run_date=self.run_date)

                print(e)
                return

            print(f"site data: {site_data}")
            print(f"type of site data: {type(site_data)}")
            print(f'adding run date {self.run_date}')
            site_data['run_date'] = self.run_date
            if self.use_kafka and self.producer:
                try:
                    # Push data to Kafka topic
                    self.producer.produce('sites', value=json.dumps(site_data), callback=kafka_callback)
                    # Flush any pending messages to Kafka
                    self.producer.flush()
                except KeyboardInterrupt:
                    print("Producer interrupted. Exiting...")
            return

    def get_site_hardware(self, site_id, site_name):
        '''Fetch site hardware from also energy, requires session cookie and site id'''
        url = f"https://api.alsoenergy.com/Sites/{site_id}/Hardware?includeArchivedFields=false&includeAlertCount=false&includeAlertInfo=false&includeDisabledHardware=false&includeSummaryFields=false&includeDeviceConfig=false&includeDataNameFields=false"

        payload = {}
        headers = {
            'accept': 'application/json',
            'Cookie': self.cookie
        }

        response = self.session.request("GET", url, headers=headers, data=payload)

        print(f"response from site hardware for site {site_id} {site_name}: {response.status_code}")
        logger.info(f"response : {response}")

        if response.status_code == requests.codes.ok:
            response = response.json()
            response['site_id'] = site_id
            response['site_name'] = site_name
            response['recordDate'] = self.run_date
            return json.dumps(response)
        
        else:
            print(f'bad response from site hardware for site {site_id}')
            if self.comms:
                logger.info(f"logging comms for site id {site_id} in the get_site_hardware function")
                print(f"logging comms for site id {site_id} in the get_site_hardware function")
                print("{entity_type}, {entity_id}, {metric}, {event}, {process}, {start_date}, {run_date}".format(entity_type='Site', 
                entity_id=site_id, 
                metric='Site Hardware', 
                event=f'HTTP Error - {response.status_code}', 
                process=self.process, 
                start_date=self.start_date, 
                run_date=self.run_date)
)
                self.comms.record_comms_event(entity_type='Site', entity_id=site_id, metric='Site Hardware', event=f'HTTP Error - {response.status_code}', process=self.process, start_date=self.start_date, run_date=self.run_date)

            return

    def produce_hardware(self, hardwares):
        if not hardwares:
            return
        if self.use_kafka and self.producer:
            try:
                # Push data to Kafka topic
                self.producer.produce('hardware', value=hardwares, callback=kafka_callback)
                # Flush any pending messages to Kafka
                self.producer.flush()
            except KeyboardInterrupt:
                print("Producer interrupted. Exiting...")
        return


    def produce_site_list(self, sites):
        '''Function to fetch site list and push to kafka topic'''
        if self.use_kafka and self.producer:
            try:
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

    # TODO: refector so that there is only one reference to the start and end date (self.start_date, self.end_date)
    # TODO: note that timestamp is "starting on" time
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
        
        response = self.session.request("GET", url, headers=headers, params=params)
        if response.status_code == requests.codes.ok:
            data = response.json()
            data["site_id"] = site_id
            data["site_name"] = site_name
            data["hardware_ids"] = hw_ids
            data["metric"] = metric_key
            data["timestamp"] = start_date.strftime("%Y-%m-%d") if hasattr(start_date, 'strftime') else str(start_date)
            data["run_date"] = self.run_date
            return json.dumps(data)
        elif response.status_code != 204:
            print(f"Error fetching custom metric data for {metric_key} from {site_id} {site_name} / {hw_ids} : {response.status_code}") 
            if self.comms:
                self.comms.record_comms_event(entity_type='Site', entity_id=site_id, metric=metric_key, event=f'HTTP Error - {response.status_code}', process=self.process, start_date=self.start_date, run_date=self.run_date)


            for hw_id in hw_ids.split(","):

                if hw_id is not None and hw_id != '' and self.comms:
                    self.comms.record_comms_event(entity_type='Hardware', entity_id=hw_id, metric=metric_key, event=f'HTTP Error - {response.status_code}', process=self.process, start_date=self.start_date, run_date=self.run_date)

            return None

    # TODO: DO NOT update start and end date to be self.start_date and self.end_date. This section needs it's own start date because it's looping and will be different than self.start_date
    def produce_hardware_metrics(self, metric, site, hardwares, start_date, end_date):
        '''Function to fetch metrics and push to kafka topic'''



        site_id = site['siteId']
        site_name = site['siteName']


        if not hardwares:
            print(f"[{metric}] No hardware retrieved for site {site_id} {site_name} for date {start_date}")
            # logger.log_site(site_id, site_name, metric, [], "fail", f"No hardware retrieved for metric: {metric}")
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
                for hw_ids in hw_groups:
                    hw_ids_str = ",".join([str(x) for x in hw_ids])
                    chart_data = self.get_hardware_metrics(metric, site_id, site_name, hw_ids_str, start_date, end_date)
                    if chart_data:
                        if self.use_kafka and self.producer:
                            print(f"Producing custom chart data for {metric} from site {site_id} {site_name}, hardware {hw_ids_str} on date {start_date}")
                            # Push data to Kafka topic
                            self.producer.produce('hardware_metrics', value=chart_data, callback=kafka_callback)
                            self.producer.flush()

                        if self.comms:
                            self.comms.record_comms_event(entity_type='Site', entity_id=site_id, metric=metric, event=f'Successful insertion', process=self.process, start_date=start_date, run_date=self.run_date)


                        print(f"Produced custom chart data for {metric} from site {site_id} {site_name}, hardware {hw_ids}, start date {start_date}")
                        # logger.log_site(site_id, site_name, metric, [], "success")
                    else:
                        print(f"Received empty response for {metric} from site {site_id} {site_name}, hardware {hw_ids}, start date {start_date}")
                        # logger.log_site(site_id, site_name, metric, hw_ids, "null", f"Empty response for metric {metric}")	
            except Exception as e:
                print(f"error fetching custom metric data for {metric} for site {site_id} {site_name}, hardware {hw_ids}: {e}, start date {start_date}")
                # logger.log_site(site_id, site_name, metric, hw_ids, "fail", f"{metric} error: {e}")
                return
        return

    def get_data(self, start_date=None, end_date=None):
        ''' Function to handle logic of grabbing data from AlsoEnergy
        
            Args:
                start_date (str): Start date in YYYY-MM-DD format (default: yesterday)
                end_date (str): End date in YYYY-MM-DD format (default: today)
        '''


        try:

            # Use provided dates or default to yesterday
            if start_date is None or end_date is None:
                today = datetime.now()
                yesterday = today - timedelta(days=1)
                start_date = yesterday.strftime("%Y-%m-%d")  # Midnight yesterday
                end_date = today.strftime("%Y-%m-%d")  # Midnight now
                self.process = 'daily'
            else:
                # Validate date format
                try:
                    start_date = datetime.strptime(start_date, "%Y-%m-%d")
                    end_date = datetime.strptime(end_date, "%Y-%m-%d")
                    # Update process type for backfill
                    self.process = 'backfill'
                except ValueError:
                    raise ValueError("Dates must be in YYYY-MM-DD format")
                
                

            # TODO: differentiate between daily and backfill processes
            # daily process should run with for yesterday
            # the backfill process should loop through the dates in the date range
            # and produce data for each date

            print('getting sites')
            sites_raw = self.get_sites()  # API call to grab list of sites
            self.produce_site_list(sites_raw)  # Push site list to kafka feed
            sites = json.loads(sites_raw)['items']

            metrics = list(self.METRIC_HARDWARE_MAP.keys())  # List of metrics we are collecting
            # logger = LogWriter()
            
            for site in sites:
                site_id = site['siteId']
                site_name = site['siteName']
                print(f"getting hardware for site {site_id} {site_name}")
                # Get hardware for that site
                try:
                    hardwares = self.get_site_hardware(site_id, site_name)
                except Exception as e:
                    print(f"error fetching site hardware for site {site_id} {site_name}")
                    print(e)
                    # logger.log_site(site_id, site_name, "all", [], "fail", f"Hardware fetch error: {e}")
                    continue

                # TODO: add dates
                self.produce_sites(site_id, site_name)  # Push information for site
                if self.comms:
                    self.comms.record_comms_event(entity_type='Site', entity_id=site_id, metric='Site Info', event=f'Successful insertion', process=self.process, start_date=self.start_date, run_date=self.run_date)

                self.produce_hardware(hardwares)  # Push hardware associated with site
                if self.comms:
                    self.comms.record_comms_event(entity_type='Site', entity_id=site_id, metric='Harware Associations', event=f'Successful insertion', process=self.process, start_date=self.start_date, run_date=self.run_date)


                # Loop through every metric and produce for that site
                print(f'date is {start_date} and type {type(start_date)}')
                print(f'date is {end_date} and type {type(end_date)}')
                for metric in metrics:
                    if self.process == 'backfill':
                        date = start_date
                        while date < end_date:
                            logger.info(f"Producing {metric} for {site_id} {site_name} for date {date - timedelta(days=1)} to {date}")
                            self.produce_hardware_metrics(metric, site, hardwares, date - timedelta(days=1), date)
                            date = date + timedelta(days=1)
                    else:
                        logger.info(f"Producing {metric} for {site_id} {site_name} for date {start_date} to {end_date}")
                        self.produce_hardware_metrics(metric, site, hardwares, start_date, end_date)

                    # Write the log to a sheet
                    # logger.write_sheet(start_date, end_date)
                

        except Exception as e:
            print(f'error in get data function: {e}')
            run_info = json.dumps({
                'timestamp': self.run_date,
                'runID': self.RUN_ID,
                'process': self.process,
                'event': 'failed'
            })
            if self.use_kafka and self.producer:
                print("Error occurred during data fetching, sending failure event to Kafka...")
                print(f"run info: {run_info}")
                self.producer.produce('solarbi_runs', value=run_info, callback=kafka_callback)  # Push Run failure to Kafka
                self.producer.flush()
            exit()
          
        # Done
        run_info = json.dumps({
            'timestamp': str(datetime.now()),
            'runID': self.RUN_ID,
            'process': self.process,
            'event': 'finished'

        })

        if self.use_kafka and self.producer:
            print("Data fetching completed successfully, sending finished event to Kafka...")
            self.producer.produce('solarbi_runs', value=run_info, callback=kafka_callback)  # Push Run end to Kafka
            self.producer.flush()


	

def main():
    '''entrypoint'''
    
    parser = argparse.ArgumentParser(description='Solar BI Site Fetcher - Fetch data from AlsoEnergy API')
    parser.add_argument('--start-date', type=str, help='Start date in YYYY-MM-DD format (default: yesterday)')
    parser.add_argument('--end-date', type=str, help='End date in YYYY-MM-DD format (default: today)')
    
    args = parser.parse_args()

    fetcher = SiteFetcher()  # Initialize object

    # Perform data extraction with optional date range
    fetcher.get_data(start_date=args.start_date, end_date=args.end_date)


if __name__ == '__main__':
    main()
