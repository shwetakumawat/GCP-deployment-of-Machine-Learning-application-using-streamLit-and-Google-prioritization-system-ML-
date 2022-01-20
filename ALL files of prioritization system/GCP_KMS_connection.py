# Import the client library.
from google.cloud import kms
from google.oauth2 import service_account
import logging
import json

class GCP_KMS_connection():

    def __init__(self):
        #CONNECT TO BIGQUERY
        #JSON CONTAINING BIGQUERY SERVICE ACCOUNT CREDENTIALS
        try :
            self.client = kms.KeyManagementServiceClient()
        except Exception as e:
            logging.error("function: kms_connection --- error : {}".format(str(e)))

    def decryption(self, encrypted_config, encrypted_credentials):

        # Build the key name.
        key_name = self.client.crypto_key_path('shared-keymgmt-production','global', 'ml-shared', 'dev')

        # Call the API.
        decrypt_response = self.client.decrypt(request={'name': key_name, 'ciphertext': encrypted_config})
        file_config = decrypt_response.plaintext.decode("utf-8")

        # Call the API.
        decrypt_response = self.client.decrypt(request={'name': key_name, 'ciphertext': encrypted_credentials})
        file_cred = decrypt_response.plaintext.decode("utf-8")
        file_cred = json.loads(file_cred)

        return file_config, file_cred
