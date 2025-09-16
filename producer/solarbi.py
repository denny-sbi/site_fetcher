import json
from datetime import datetime, timedelta
from utils import kafka_callback
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SolarBIComms:
    def __init__(self, producer, run_id):

        self.RUN_ID = run_id
        self.producer = producer


    def record_comms_event(self, entity_type, entity_id, metric, event, process, start_date, run_date):
        try:
            run_info = json.dumps({
                'timestamp': str(start_date),
                'runID': self.RUN_ID,
                'entityType': entity_type,
                'entityID': entity_id,
                'metric': metric,
                'event': event,
                'run_date': str(run_date),
                'process': process,
            })
            logger.info(f"Recording comms event: {run_info}")

            self.producer.produce('solarbi_comms', value=run_info, callback=kafka_callback)  # Push Run start to Kafka
            self.producer.flush()
        except Exception as e:
            logger.error(f"Error recording comms event: {e}")
            logger.error(f"entity type: {entity_type}, entity id: {entity_id}, metric: {metric}, event: {event}, process: {process}, start date: {start_date}, run date: {run_date}")