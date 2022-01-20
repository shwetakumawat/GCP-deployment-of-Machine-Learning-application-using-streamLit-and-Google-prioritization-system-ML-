from datetime import datetime
import pickle
import json
import numpy as np


def preprocessing_of_the_request(input_params, API_data, lookup_pest):

     #FROM THE INPUT REQUEST----------------------------------------------------

     #RETRIEVE TODAY'S DATE AND PARSING IT
     sin_week_day, cos_week_day, cos_day_month, sin_day_month = parse_date(datetime.today().strftime('%m/%d/%Y'))

     #RETRIEVE TYPE OF PEST, ENCODED USING A ONE-HOT ARRAY
     pest = conversion_pest_to_1hot(input_params["pest_prob"], lookup_pest)

     #RETRIEVE IF IT IS A KNOWN CUSTOMER
     exist = float(eval((input_params["exist"]).capitalize()))

     #FROM API -----------------------------------------------------------------

     #RETRIEVE WHEN THE HOUSE WAS BUILT
     year_built = API_data['structure']['year_built']

     #RETRIEVE LATITUDE AND LONGITUDE
     lat = API_data['address']['latitude']
     lon = API_data['address']['longitude']

     #RETRIEVE SQUARE FOOTAGE
     square_footage = API_data['structure']['total_area_sq_ft']

     #RETRIEVE NUMBER OF STORIES (IF THE VALUE IS UNAVAILABLE SET TO 2)
     stories = API_data['structure']['stories'] if API_data['structure']['stories'] != None else 2

     #CHECKING THE TYPE OF STORIES
     stories = str(stories)
     stories = stories.split('+')
     if len(stories)>1:
         stories_first_element = round(float(stories[0]))
         stories_second_element = len(stories[1])
         stories = stories_first_element + stories_second_element
     else:
        stories = stories[0]


     #RETRIEVE NUMBER OF ROOMS
     rooms = conversion_rooms_number_to_1_hot(API_data['structure']["rooms_count"])



     return np.array([exist,
                      sin_week_day,
                      cos_week_day,
                      cos_day_month,
                      sin_day_month,
                      lat,
                      lon,
                      float(square_footage),
                      float(stories),
                      int(year_built),
                      *pest,
                      *rooms])


def parse_date(date):

  datetime_object = datetime.strptime(date, '%m/%d/%Y')

  day = datetime_object.day
  week_day = datetime_object.weekday()

  sin_week_day = np.sin(2*np.pi*week_day/7)
  cos_week_day = np.cos(2*np.pi*week_day/7)

  sin_day_month = np.sin(2*np.pi*day/30)
  cos_day_month = np.cos(2*np.pi*day/30)


  return sin_week_day, cos_week_day, cos_day_month, sin_day_month


def conversion_rooms_number_to_1_hot(customer_number_of_rooms):

    # ONE_HOT VECTOR TO REPRESENT THE NUMBER OF ROOMS [0,0,0]
    # rooms_count     '0' : >=6 ,
    #                 '1' : <6,
    #                 '2' : Unknown
    one_hot_rooms = np.zeros(3)
    if customer_number_of_rooms is None:
        idx = 2
    elif int(customer_number_of_rooms) <= 6:
        idx = 1
    else:
        idx = 0
    np.put(one_hot_rooms,idx,1)

    return one_hot_rooms


def conversion_pest_to_1hot(customer_pest, lookup_pest):
    one_hot_pest = np.zeros(len(lookup_pest))
    try:
        idx = lookup_pest.index(customer_pest)
    except ValueError:
        idx = 4
    np.put(one_hot_pest,idx,1)

    return one_hot_pest
