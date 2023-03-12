# Import libraries
import sqlite3
from contextlib import closing
from datetime import datetime
import os
# get filepath to database
parent_dir = os.path.dirname(os.getcwd())
db_filepath = parent_dir + r'\ufc_db.db'

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# FUNCTIONS FOR EXECUTING QUERIES
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Fetch one ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def ex_query_fetch_one(query, param):
  with closing(sqlite3.connect(db_filepath)) as db_connection:
    with closing(db_connection.cursor()) as cursor:
      to_return = cursor.execute(query, param).fetchone()
      if (to_return != None):
        return to_return[0]
      else:
        return to_return

# Fetch all  ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def ex_query_fetch_all(query, param=None):
  with closing(sqlite3.connect(db_filepath)) as db_connection:
    with closing(db_connection.cursor()) as cursor:
      # if no parameters
      if (param == None):
        to_return = cursor.execute(query).fetchall()
        if (to_return != None):
          return to_return
        else:
          return None
      # if parameter provided...
      else:
        to_return = cursor.execute(query, param).fetchall()
        if (to_return != None):
          return to_return
        else:
          return None

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# FUNCTIONS TO GET ID
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Get event id ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def get_event_id(event_name, event_date):
  query = "SELECT id FROM events WHERE name = ? AND date = ?;"
  param = [event_name, event_date]
  event_id = ex_query_fetch_one(query, param)
  return event_id

# Get fighter id (name + fight weight) ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ @@@@ Bruno Silva conflict
def get_fighter_id_fightweight(fighter_name, fight_weightclass):
  query = "SELECT id FROM fighters WHERE name = ? AND last_weight_fought = ?;"
  param = [fighter_name, fight_weightclass]
  num_results = ex_query_fetch_all(query, param)
  if (len(num_results) == 1):
    return num_results[0][0]
  else:
    return None

# Get fighter id (name + dob) ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ @@@@ Bruno Silva conflict
def get_fighter_id_dob(fighter_name, dob):
  if (dob == None):
    query = "SELECT id FROM fighters WHERE name = ? AND dob IS NULL;"
    param = [fighter_name]
  else:
    query = "SELECT id FROM fighters WHERE name = ? AND dob = ?;"
    param = [fighter_name, dob]

  fighter_id = ex_query_fetch_one(query, param)
  return fighter_id

# Get ref id ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def get_ref_id(ref_name):
  query = "SELECT id FROM referees WHERE name = ?;"
  param = [ref_name]
  ref_id = ex_query_fetch_one(query, param)
  return ref_id

# Get judge id ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def get_judge_id(judge_name):
  query = "SELECT id FROM judges WHERE name = ?;"
  param = [judge_name]
  judge_id = ex_query_fetch_one(query, param)
  return judge_id

# Get fight id (super key is event_id + 1 fighter name) ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ @@@@@@@@@@@ need to check if this works
def get_fight_id(event_id, fighter1_name, fighter2_name):
  query = "SELECT id FROM fights WHERE event_id = ? AND fighter_1_name = ? AND fighter_2_name = ?;"
  param1 = [event_id, fighter1_name, fighter2_name]
  param2 = [event_id, fighter2_name, fighter1_name]
  
  check_order1 = ex_query_fetch_one(query, param1)
  # if param order 1 is not none
  if (check_order1 != None):
    return(check_order1)

  # if param order 1 is none
  else:
    check_order2 = ex_query_fetch_one(query, param2)
    # if param order 2 is not none
    if (check_order2 != None):
      return(check_order2)
    
    # if param order 1 and 2 is none
    else:
      return None

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# FUNCTIONS TO CHECK OCCURANCE
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Check num of occurances of event name in db ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def check_occurance_event(event_name, event_date):
  # input: event_date, event_date
  # return: number of occurances of event name
  event_occ_query = "SELECT COUNT(*) FROM events WHERE name = ? AND date = ?;"
  event_occ_query_param = [event_name, event_date]
  event_occ = ex_query_fetch_one(event_occ_query, event_occ_query_param)
  return event_occ

# Check num of occurances of fighter name in db ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def check_occurance_fighter(fighter_name, dob):
  # input: fighter's name, dob
  # return: number of occurances of fighter

  if (dob == None):
    fighter_occ_query = "SELECT COUNT(id) FROM fighters WHERE name = ? AND dob IS NULL;"
    fighter_occ_query_param = [fighter_name]
  else:
    fighter_occ_query = "SELECT COUNT(id) FROM fighters WHERE name = ? AND dob = ?;"
    fighter_occ_query_param = [fighter_name, dob]

  fighter_occ = ex_query_fetch_one(fighter_occ_query, fighter_occ_query_param)
  return fighter_occ

# Check num of occurances of referee name in db ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def check_occurance_ref(ref_name):
  # input: ref name
  # return: number of occurances of ref
  ref_occ_query = "SELECT COUNT(*) FROM referees WHERE name = ?;"
  ref_occ_query_param = [ref_name]
  ref_occ = ex_query_fetch_one(ref_occ_query, ref_occ_query_param)
  return ref_occ

# Check num of occurances of judge name in db ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def check_occurance_judge(judge_name):
  # input: judge name
  # return: number of occurances of judge
  judge_occ_query = "SELECT COUNT(*) FROM judges WHERE name = ?;"
  judge_occ_query_param = [judge_name]
  judge_occ = ex_query_fetch_one(judge_occ_query, judge_occ_query_param)
  return judge_occ

# Check num of occurances of fight ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def check_occurance_fight(event_id, fighter1_name, fighter2_name):
  # input: 1) event_id 2) fighter 1 name 3) fighter 2 name
  # return: number of occurances of fights matching that

  fight_occ_query = "SELECT COUNT(*) FROM fights WHERE event_id = ? AND fighter_1_name = ? AND fighter_2_name = ?;"
  fight_occ_query_param_order_1 = [event_id, fighter1_name, fighter2_name]
  fight_occ_query_param_order_2 = [event_id, fighter2_name, fighter1_name]

  # if param order 1 is 1, return 1
  if (ex_query_fetch_one(fight_occ_query, fight_occ_query_param_order_1) == 1):
    return 1
  # if param order 1 is 0, check if param order 2 is 1
  else:
    if (ex_query_fetch_one(fight_occ_query, fight_occ_query_param_order_2) == 1):
      return 1
    # if both params are 0, return 0
    else:
      return 0

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# OTHER FUNCTIONS
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# get all fight IDs and date
def get_all_fight_id_and_date():
  query = "SELECT events.date, fights.id FROM events JOIN fights ON events.id = fights.event_id;"
  list_of_items = ex_query_fetch_all(query)
  return list_of_items

# get fight IDs for particular year ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def get_all_fight_id_for_year(year):
  # get list of all fights and dates
  all_fights = get_all_fight_id_and_date()
  # init return list
  to_return = []
  
  for fight in all_fights:
    # convert date to datetime obj
    fight_date = datetime.strptime(fight[0], '%d/%b/%Y')
    # make datetime obj for first and last day of year
    first_day_yr = datetime.strptime(f'01/Jan/{year}', '%d/%b/%Y')
    last_day_yr = datetime.strptime(f'31/Dec/{year}', '%d/%b/%Y')
    
    # if the fight day falls inbetween, append to return list
    if ((fight_date >= first_day_yr) and (fight_date <= last_day_yr)):
      to_return.append(fight[0])
  
  return to_return

# get fight IDs for year yyyy and ALL years BEFORE ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def get_all_fight_id_for_year_and_before(year):
  # get list of all fights and dates
  all_fights = get_all_fight_id_and_date()
  # init return list
  to_return = []
  
  for fight in all_fights:
    # convert date to datetime obj
    fight_date = datetime.strptime(fight[0], '%d/%b/%Y')
    # make datetime obj for first and last day of year
    # since the db only has fights from 2001 onwards, first year set to 2001
    first_day_yr = datetime.strptime(f'01/Jan/2001', '%d/%b/%Y')
    last_day_yr = datetime.strptime(f'31/Dec/{year}', '%d/%b/%Y')
    
    # if the fight day falls inbetween, append to return list
    if ((fight_date >= first_day_yr) and (fight_date <= last_day_yr)):
      to_return.append(fight[0])
  
  return to_return

# get fight IDs for year yyyy and ALL years AFTER ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def get_all_fight_id_for_year_and_after(year):
  # get list of all fights and dates
  all_fights = get_all_fight_id_and_date()
  # init return list
  to_return = []
  
  for fight in all_fights:
    # convert date to datetime obj
    fight_date = datetime.strptime(fight[0], '%d/%b/%Y')
    # make datetime obj for first and last day of year
    first_day_yr = datetime.strptime(f'01/Jan/{year}', '%d/%b/%Y')
    # ending year set to 2099
    last_day_yr = datetime.strptime(f'31/Dec/2099', '%d/%b/%Y')
    
    # if the fight day falls inbetween, append to return list
    if ((fight_date >= first_day_yr) and (fight_date <= last_day_yr)):
      to_return.append(fight[0])
  
  return to_return

# get all fight IDs ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
def get_all_fight_id():
  query = "SELECT id FROM fights;"
  list_of_ids = [x[0] for x in ex_query_fetch_all(query)]
  return list_of_ids


# compute last weightclass fought at ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
def get_last_weight_fought(fighter_id):
  # input: fighter's id
  # output: string text of weightclass
  
  # get all (date and fight_id)
  query = "SELECT events.date, fights.id FROM events JOIN fights ON events.id = fights.event_id WHERE fights.id = (SELECT fight_id FROM fight_stats WHERE fighter_id = ? AND round = 0);"
  param = [fighter_id]
  dates_and_fight_id = ex_query_fetch_all(query, param)
  
  # assign 1st tuple to be compared
  compared = dates_and_fight_id[0]
  
  # loop through and compare tuples (if only 1 element, the whole loop will skip)
  for i in range(1, len(dates_and_fight_id)):
    # compare dates after changing both to date time objects. If 'compared' is before iterated fight, change it out
    if ( datetime.strptime(compared[0], '%d/%b/%Y') < datetime.strptime(dates_and_fight_id[i][0], '%d/%b/%Y') ):
      compared = dates_and_fight_id[i]
  
  # after comparing, query and return weightclass for that fight
  new_query = "SELECT weightclass FROM fights WHERE id = ?;"
  new_param = [compared[1]]
  return ex_query_fetch_one(new_query, new_param)



# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def main():
  print(len(get_all_fight_id_for_year(2020)))

if __name__ == "__main__":
  main()