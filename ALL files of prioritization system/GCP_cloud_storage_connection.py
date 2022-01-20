
from google.cloud import storage
from google.oauth2 import service_account

import logging

class GCP_cloud_storage_connection():

    def __init__(self):
        #CONNECT TO BIGQUERY
        #JSON CONTAINING BIGQUERY SERVICE ACCOUNT CREDENTIALS
        try :
            self.client = storage.Client()
        except Exception as e:
            logging.error("function: cloud_storage_connection --- error : {}".format(str(e)))

    def get_files_from_bucket(self):

        bucket = self.client.bucket('ml-shared-dev.appspot.com')
        # get bucket data as blob
        blob_1 = bucket.blob('config_enc.ini')
        blob_1_bytes = blob_1.download_as_bytes()

        blob_2 = bucket.blob('ml-shared-dev-3665933ac429_enc.json')
        blob_2_bytes = blob_2.download_as_bytes()

        return blob_1_bytes, blob_2_bytes
