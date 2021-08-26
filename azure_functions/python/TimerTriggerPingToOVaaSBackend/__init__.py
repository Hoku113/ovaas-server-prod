import datetime
import logging

import azure.functions as func

import requests
import os

img = "./sample.jpg"
def run():
    with open(img, "rb") as f:
        files = {'image':('sample.jpg', f, 'image/jpeg', {})}
        r = requests.post(os.environ.get("TARGET_ENDPOINT"), files=files)
        logging.info(r.status_code)

def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')
    
    logging.info("started")
    run()
    logging.info('start next')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
