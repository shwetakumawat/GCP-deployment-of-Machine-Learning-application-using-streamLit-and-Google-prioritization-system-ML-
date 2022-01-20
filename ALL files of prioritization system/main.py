from flask import Flask, render_template, request, jsonify

#IMPORTING MODULES FROM OTHER SCRIPTS
from preprocessing import preprocessing_of_the_request
from GCP_big_query_connection import GCP_big_query_connection
from GCP_cloud_storage_connection import GCP_cloud_storage_connection
from GCP_KMS_connection import GCP_KMS_connection
from counter_check import check_daily_counter_table
from ml_model import Model

import uuid
import numpy as np
import requests
import configparser
import logging
import json
import timeit
from datetime import datetime
import base64
import re
import ast



app = Flask(__name__)



#INITIALIZING GCP STORAGE CONNECTION -ONE TIME
#RETURN THE ENCRYPTED CONFIG.INI FILE AS A STRING
GCP_cloud_storage_client = GCP_cloud_storage_connection()
ENCRYPTED_CONFIG_FILE, ENCRYPTED_CREDENTIALS_FILE = GCP_cloud_storage_client.get_files_from_bucket()

#INITIALIZING GC KMS -ONE TIME
#RETURN THE CONFIG.INI DECRYPTED
GCP_KMS_client = GCP_KMS_connection()
CONFIG_FILE, CREDENTIALS_FILE = GCP_KMS_client.decryption(ENCRYPTED_CONFIG_FILE, ENCRYPTED_CREDENTIALS_FILE)


#INITIALIZING CONFIG PARSER -ONE TIME
CONFIG = configparser.ConfigParser()
CONFIG.read_string(CONFIG_FILE)

#INITIALIZING GCP BIG QUERY CONNECTION -ONE TIME
GCP_big_query_client = GCP_big_query_connection(CONFIG, CREDENTIALS_FILE)



#INITIALIZING ML MODEL -ONE TIME
MODEL_OBJ = Model()
MODEL_RF = MODEL_OBJ.get_model()

#INITIALIZING VARIABLE FROM CONFIG ONE TIME
API_URL = CONFIG['estated_API']['URL']
API_TOKEN = CONFIG['estated_API']['token']
THRESHOLD_0 = CONFIG['predictions_threshold']['threshold_0']
THRESHOLD_0_5 = CONFIG['predictions_threshold']['threshold_0_5']
THRESHOLD_1 = CONFIG['predictions_threshold']['threshold_1']
THRESHOLD_1_5 = CONFIG['predictions_threshold']['threshold_1_5']
THRESHOLD_2 = CONFIG['predictions_threshold']['threshold_2']
DB_SCORES_TABLE = CONFIG['google_cloud']['db_scores_table']
DB_APPOINTMENT_TABLE  = CONFIG['google_cloud']['db_appointment_table']
DB_ERROR_TABLE = CONFIG['google_cloud']['db_error_table']

#INITIALIZING PREDICTION_LOOKUPS VARIABLE
LOOKUP_PEST = eval(CONFIG['predictions_lookup']['lookup_pest'])

#CHECK IF THE VOCABULARY IS COMPLETE
KEYS = ['pest_prob', 'exist', 'address_line_1', 'town', 'state']
KEYS_TICKET = ['prediction_id', 'ticket_id']



"""
API entrypoint, main method which analyze
the request in input

@param request : dictionary
@... be sure of how many fields are


@return prediction_id : string
@return push_back_in_calendar : int


"""
@app.route("/entrypoint", methods = ['GET', 'POST'])
def analyzing_request():

    start_1 = timeit.default_timer()

    use_model=False
    reason = "N/A"


    #READING THE POST REQUEST FROM THE CCM WEBSITE
    input_params=request.get_json(force=True)

    #CHECKING REQUEST CORRECTNESS
    for key in KEYS:
        if key not in input_params.keys():
            reason = "There is an error in the input request, specifically the field {} is missing".format(key)
            fields_array = [json.dumps(input_params), reason, datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")]
            GCP_big_query_client.insert_query(DB_ERROR_TABLE, fields_array, CONFIG).result()
            return jsonify(prediction_ID="N/A", use_Prediction=False, push_back=0, time=0, reason = reason)

        else:
            if not input_params[key]:
                reason = "There is an error in the input request, specifically the field {} is empty".format(key)
                fields_array = [json.dumps(input_params), reason, datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")]
                GCP_big_query_client.insert_query(DB_ERROR_TABLE, fields_array, CONFIG).result()
                return jsonify(prediction_ID="N/A", use_Prediction=False, push_back=0, time=0, reason = reason)

    #CHECKING THAT THE IT IS A WEST COAST REQUEST:
    if input_params['state'] not in ['California', 'Nevada', 'Arizona']:
        reason = "The request is not coming from a west coast state"
        fields_array = [json.dumps(input_params), reason, datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")]
        GCP_big_query_client.insert_query(DB_ERROR_TABLE, fields_array, CONFIG).result()
        return jsonify(prediction_ID="N/A", use_Prediction=False, push_back=0, time=0, reason = reason)



    #GENERATING ID OF THE PREDICTION
    prediction_ID = uuid.uuid1().hex
    address = ', '.join([input_params['address_line_1'], input_params['town'], input_params['state']])

    #CALL THE ESTATED API, TO RETRIEVE HOUSE DETAILS
    try:
        response = requests.get('{}{}&combined_address={}'.format(API_URL,API_TOKEN,address))
        response = json.loads(response.content)
    except Exception as e:
        reason = "No response from the Estated API, check if the Estated API is up and running"
        fields_array = [json.dumps(input_params), reason, datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")]
        GCP_big_query_client.insert_query(DB_ERROR_TABLE, fields_array, CONFIG).result()
        return jsonify(prediction_ID="N/A", use_Prediction=False, push_back=0, time=0, reason= reason)

    try:
        if response['error']:
            reason = response['error']['description']
            fields_array = [json.dumps(input_params), reason, datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")]
            GCP_big_query_client.insert_query(DB_ERROR_TABLE, fields_array, CONFIG).result()
            return jsonify(prediction_ID="N/A", use_Prediction=False, push_back=0, time=0, reason = reason)
    except Exception as e:
        pass

    #IF THE API RETURNS DATA FOR THE INPUT ADDRESS
    if response['data'] != None:

        #SOMETIMES YEAR_BUILT AND TOTAL_AREA_SQ_FT ARE MISSING FROM THE ESTATED API SO LET'S DO A CHECK
        if response['data']['structure']['year_built'] != None and response['data']['structure']['total_area_sq_ft'] != None:

            #BUILDING THE FEATURE LIST
            feature_list  = preprocessing_of_the_request(input_params, response['data'], LOOKUP_PEST)

            #PREDICTION OF THE ACTUAL PROPENSITY VALUE
            prob_of_selling = round(MODEL_RF.predict_proba(feature_list.reshape(1,-1))[0][1],2)

            #BASED ON THE THRESHOLD WE DECIDE WHETHER OR NOT TO USE THE MODEL
            if prob_of_selling >= float(THRESHOLD_0):
                push_back_in_calendar = 0
            elif prob_of_selling >= float(THRESHOLD_0_5):
                push_back_in_calendar = 3
            elif prob_of_selling >= float(THRESHOLD_1):
                push_back_in_calendar = 5
            elif prob_of_selling >= float(THRESHOLD_1_5):
                push_back_in_calendar = 7
            elif prob_of_selling >= float(THRESHOLD_2):
                push_back_in_calendar = 10
            else:
                push_back_in_calendar = None
                reason = "Propensity not within defined thresholds"

            #IF THE THRESHOLD WAS BIG ENOUGH WE CREATED THE PUSH_BACK_IN_CALENDAR
            #VARIABLE AND WE TRY TO UPDATE THE COUNTER
            if push_back_in_calendar is not None:

                use_model, reason = check_daily_counter_table(GCP_big_query_client, CONFIG)

            else:
                use_model = False


            #SAVE TO THE SCORES_TABLE
            feature_list = list(feature_list)
            feature_list.insert(0, prediction_ID)
            feature_list.extend([push_back_in_calendar, float(use_model), reason, prob_of_selling,  datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")])
            GCP_big_query_client.insert_query(DB_SCORES_TABLE, feature_list, CONFIG).result()


        else:
            use_model = False
            reason = 'Estated API returned some null values'
            fields_array = [json.dumps(input_params), reason, datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")]
            GCP_big_query_client.insert_query(DB_ERROR_TABLE, fields_array, CONFIG).result()


    else:
        use_model = False
        reason = 'No property was found at the given address'
        fields_array = [json.dumps(input_params), reason, datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")]
        GCP_big_query_client.insert_query(DB_ERROR_TABLE, fields_array, CONFIG).result()

    stop_1 = timeit.default_timer()

    if not use_model:
        push_back_in_calendar = 0

    return jsonify(prediction_ID=prediction_ID, use_Prediction=use_model, push_back=push_back_in_calendar, time=str(stop_1-start_1), reason = reason)



@app.route("/successful_appointment", methods = ['GET', 'POST'])
def insert_ticket_id():
    logging.error("We are hereeee")
    #READING THE POST REQUEST FROM THE CCM WEBSITE
    envelope = json.loads(request.data.decode('utf-8'))
    #CONVERTING TO ASCII
    convert_to_ascii = base64.b64decode(envelope['message']['data'].encode('ascii')).decode("utf-8")
    #CONVERT THE STRING IN A JSON OBJ

    logging.error(convert_to_ascii)
    
    #data = ast.literal_eval(convert_to_ascii)

    #try this workaround
    #if not isinstance(data,str):
    #    data = str(data)
    
    data = json.loads(convert_to_ascii)
    
    logging.error(data)

    fields = [data['prediction_id'],data['ticket_id'],datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")]

    try:
        GCP_big_query_client.insert_query(DB_APPOINTMENT_TABLE, fields, CONFIG).result()
        result = True
    except Exception as e:
        logging.error("function: insert_ticket_id --- error : {}".format(str(e)))
        result = False
    return jsonify(result=result)
