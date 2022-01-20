from datetime import date
import random
import logging



"""
Retrieve the current counter value,
decide to update or not based
on the counter_value, the date and the request

@return send_back_prediction : bool


"""
def check_daily_counter_table(GCP_big_query_client, config):


    # RANDOM FUNCTION
    # BASED ON THE "RANDOM" VALUE DECIDE IF WE WANNA RETURN BACK THE PREDICTION
    # TO THE CCM
    if random.random() > 0.5:
        send_prediction_to_CCM = True
    else:
        send_prediction_to_CCM = False
        reason = "The prediction is not sent to back to the CCM "

    #WE CAN HAVE :
    # -MAXIMUM REACHED AT THE CURRENT DATE ---> Do not update anything
    # -MAXIMUM NOT REACHED AT THE CURRENT DATE ---> Update just the value -1
    # -MAXIMUM REACHED AT A PAST DATE ---> Update the value and the date

    if send_prediction_to_CCM:

        dataset = config['google_cloud']['dataset']
        db_counter_table = config['google_cloud']['db_counter_table']



        #THE RESULTS WILL CONTAIN SOMETHING LIKE COUNTER_VALUE/ DATE
        results = GCP_big_query_client.retrieve_daily_counter(db_counter_table, dataset).result().to_dataframe()
        reason = "everything correct"
    

        if results.empty:
            #THE BIGQUERY TABLE IS EMPTY SO WE CREATE A NEW ROW IN THE COUNTER TABLE
            query_to_execute = GCP_big_query_client.insert_new_daily_counter_row(db_counter_table, dataset, config['counter_value']['maximum_value_counter'])

        elif results.iloc[0]['refresh_date'] != date.today():
            #IS THE FIRST PREDICTION OF THE DAY SO WE ARE GOING
            #TO CREATE A NEW ROW IN THE COUNTER TABLE
            query_to_execute = GCP_big_query_client.insert_new_daily_counter_row(db_counter_table, dataset, config['counter_value']['maximum_value_counter'])


        elif results.iloc[0]['counter'] > 0:
            #MAXIMUM NOT REACHED AT THE CURRENT DAY
            query_to_execute = GCP_big_query_client.update_current_daily_counter(db_counter_table, dataset, int(results.iloc[0]['counter'] - 1))


        else:
            #DO NOT UPDATE ANYTHING !!! MAXIMUM REACHED AT THE CURRENT DAY  results['counter'] == 0 => True
            send_prediction_to_CCM = False
            reason = "Counter has reached 0 today, no more available prediction for the current day"

    else:
        pass
        #WE DO NOT HAVE TO SEND BACK TO CCM THE PREDICTION SO WE DO NOT HAVE TO UPDATE THE COUNTER

    try:
        if send_prediction_to_CCM:
            query_to_execute.result()
    except Exception as e:
        logging.error("function: maximum_counter_check --- error : {}".format(str(e)))


    return send_prediction_to_CCM, reason
