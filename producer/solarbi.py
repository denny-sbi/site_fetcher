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


    def record_comms_event(self, entity_type, entity_id, metric, event):
        run_info = json.dumps({
            'timestamp': str(datetime.now()),
            'runID': self.RUN_ID,
            'entityType': entity_type,
            'entityID': entity_id,
            'metric': metric,
            'event': event
        })

        self.producer.produce('solarbi_comms', value=run_info, callback=kafka_callback)  # Push Run start to Kafka
        self.producer.flush()