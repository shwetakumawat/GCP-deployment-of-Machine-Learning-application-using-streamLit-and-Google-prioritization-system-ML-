from google.cloud import bigquery
from google.oauth2 import service_account

import logging
from datetime import date

"""

Class which contains all methods related to GC big query serivce

"""
class GCP_big_query_connection():


    """
    Establish the connection to the db in GBQ

    @return client : Google BQ instance

    """
    def __init__(self, config, credentials_json):
        #CONNECT TO BIGQUERY
        #JSON CONTAINING BIGQUERY SERVICE ACCOUNT CREDENTIALS
        try :
            self.credentials = service_account.Credentials.from_service_account_info(credentials_json)
            self.client = bigquery.Client(credentials= self.credentials,project=config['google_cloud']['gcp_project'])
        except Exception as e:
            logging.error("function: database_connection --- error : {}".format(str(e)))




    def insert_query(self,db_table,feature_list, config):
        query_to_execute = self.client.query("""
            INSERT INTO {}.{}
            VALUES {}
            """.format(config['google_cloud']['dataset'],
                       db_table,
                       tuple(feature_list)))

        return query_to_execute

    def retrieve_daily_counter(self, db_table, dataset):
        query_to_execute = self.client.query("""
            select * from {}.{}
            where refresh_date = (select MAX(refresh_date) from {}.{})
            """.format(dataset,
                       db_table,
                       dataset,
                       db_table))
        return query_to_execute

    def insert_new_daily_counter_row(self, db_table, dataset, max):
        query_to_execute = self.client.query("""
            INSERT INTO {}.{}
            VALUES ({},DATE('{}'),{})
            """.format(dataset,
                       db_table,
                       int(max) - 1,
                       str(date.today()),
                       int(max)))
        return query_to_execute


    def update_current_daily_counter(self, db_table, dataset, current_counter):
        query_to_execute = self.client.query(""" UPDATE {}.{}
                                                 SET counter = {}
                                                 WHERE refresh_date = DATE('{}')
                                            """.format(dataset, db_table, current_counter, str(date.today())))

        return query_to_execute
