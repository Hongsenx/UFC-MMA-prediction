import sqlite3
from datetime import datetime
import re
from sqlite3 import OperationalError
import os
# for accessing 'ufc_database.db'
import sys
sys.path.append("../")
# for error logging
import traceback
import scraper_functions
# import closing for easy clean up of db stuff
from contextlib import closing
# logging ~~~~
import logging
logger = logging.getLogger(__name__)
'''
def set_logger():
  logging.basicConfig(
    level=logging.INFO,
    format="{asctime} {levelname:<8} {message}",
    style = "{",
    filemode="a+")

  # create logger
  logger = logging.getLogger()
  handler = logging.FileHandler("db.log")
  formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
  handler.setFormatter(formatter)
  logger.addHandler(handler)

  return logger
logger = set_logger()
'''

class db_updater():
  def __init__(self, database_file_path_name):
    self.database = database_file_path_name
    self.cursor = None

  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # FUNCTIONS TO GET ID
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

  # Get event id ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  def get_event_id(self, event_name, event_date):
    query = "SELECT id FROM events WHERE name = ? AND date = ?;"
    param = [event_name, event_date]
    event_id = self.cursor.execute(query, param).fetchone()[0]
    return event_id

  # Get fighter id (name + fight weight) ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  def get_fighter_id_fightweight(self, fighter_name, fight_weightclass):
    query = "SELECT id FROM fighters WHERE name = ? AND last_weight_fought = ?;"
    param = [fighter_name, fight_weightclass]
    num_results = self.cursor.execute(query, param).fetchmany()
    if (len(num_results) == 1):
      return num_results[0][0]
    else:
      return None

  # Get fighter id (name + dob) ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  def get_fighter_id_dob(self, fighter_name, dob):
    if (dob == None):
      query = "SELECT id FROM fighters WHERE name = ? AND dob IS NULL;"
      param = [fighter_name]
    else:
      query = "SELECT id FROM fighters WHERE name = ? AND dob = ?;"
      param = [fighter_name, dob]

    fighter_id = self.cursor.execute(query, param).fetchone()[0]
    return fighter_id

  # Get ref id ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  def get_ref_id(self, ref_name):
    query = "SELECT id FROM referees WHERE name = ?;"
    param = [ref_name]
    ref_id = self.cursor.execute(query, param).fetchone()[0]
    return ref_id

  # Get judge id ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  def get_judge_id(self, judge_name):
    query = "SELECT id FROM judges WHERE name = ?;"
    param = [judge_name]
    judge_id = self.cursor.execute(query, param).fetchone()[0]
    return judge_id

  # Get fight id (super key is event_id + 1 fighter name) ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  def get_fight_id(self, event_id, fighter1_name, fighter2_name):
    query = "SELECT id FROM fights WHERE event_id = ? AND fighter_1_name = ? AND fighter_2_name = ?;"
    param1 = [event_id, fighter1_name, fighter2_name]
    param2 = [event_id, fighter2_name, fighter1_name]
    
    # if param order 1 is not none
    if (self.cursor.execute(query, param1).fetchone() != None):
      return(self.cursor.execute(query, param1).fetchone()[0])

    # if param order 1 is none
    else:
      # if param order 2 is not none
      if (self.cursor.execute(query, param2).fetchone() != None):
        return(self.cursor.execute(query, param2).fetchone()[0])
      
      # if param order 1 and 2 is none
      else:
        return None

  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # FUNCTIONS TO CHECK OCCURANCE
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

  # Check num of occurances of event name in db ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  def check_occurance_event(self, event_name, event_date):
    # input: event_date, event_date
    # return: number of occurances of event name
    event_occ_query = "SELECT COUNT(*) FROM events WHERE name = ? AND date = ?;"
    event_occ_query_param = [event_name, event_date]
    event_occ = self.cursor.execute(event_occ_query, event_occ_query_param).fetchone()[0]
    return event_occ

  # Check num of occurances of fighter name in db ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  def check_occurance_fighter(self, fighter_name, dob):
    # input: fighter's name, dob
    # return: number of occurances of fighter
    
    if (dob == None):
      fighter_occ_query = "SELECT COUNT(id) FROM fighters WHERE name = ? AND dob IS NULL;"
      fighter_occ_query_param = [fighter_name]
    else:
      fighter_occ_query = "SELECT COUNT(id) FROM fighters WHERE name = ? AND dob = ?;"
      fighter_occ_query_param = [fighter_name, dob]

    fighter_occ = self.cursor.execute(fighter_occ_query, fighter_occ_query_param).fetchone()[0]
    return fighter_occ

  # Check num of occurances of referee name in db ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  def check_occurance_ref(self, ref_name):
    # input: ref name
    # return: number of occurances of ref
    ref_occ_query = "SELECT COUNT(*) FROM referees WHERE name = ?;"
    ref_occ_query_param = [ref_name]
    ref_occ = self.cursor.execute(ref_occ_query, ref_occ_query_param).fetchone()[0]
    return ref_occ

  # Check num of occurances of judge name in db ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  def check_occurance_judge(self, judge_name):
    # input: judge name
    # return: number of occurances of judge
    judge_occ_query = "SELECT COUNT(*) FROM judges WHERE name = ?;"
    judge_occ_query_param = [judge_name]
    judge_occ = self.cursor.execute(judge_occ_query, judge_occ_query_param).fetchone()[0]
    return judge_occ

  # Check num of occurances of fight ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  def check_occurance_fight(self, event_id, fighter1_name, fighter2_name):
    # input: 1) event_id 2) fighter 1 name 3) fighter 2 name
    # return: number of occurances of fights matching that
    
    fight_occ_query = "SELECT COUNT(*) FROM fights WHERE event_id = ? AND fighter_1_name = ? AND fighter_2_name = ?;"
    fight_occ_query_param_order_1 = [event_id, fighter1_name, fighter2_name]
    fight_occ_query_param_order_2 = [event_id, fighter2_name, fighter1_name]
    
    # if param order 1 is 1, return 1
    if (self.cursor.execute(fight_occ_query, fight_occ_query_param_order_1).fetchone()[0] == 1):
      return 1
    # if param order 1 is 0, check if param order 2 is 1
    else:
      if (self.cursor.execute(fight_occ_query, fight_occ_query_param_order_2).fetchone()[0] == 1):
        return 1
      # if both params are 0, return 0
      else:
        return 0

  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # INSERTING FUNCTIONS (FUNCTIONS TO WRITE TO DB)
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

  # Insert event in db ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  def insert_event_in_db(self, event):
    # input: an event dict with basic details 1) event name 2) date 3) country 4) state
    event_insert_query = "INSERT INTO events (name, date, country, state) VALUES (?, ?, ?, ?);"
    event_insert_query_param = [event['name'], event['date'], event['country'], event['state']]
    self.cursor.execute(event_insert_query, event_insert_query_param)

  # get dict entries from fighter profile JSON files where the name matches ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  def get_fighter_dict_in_json_file(self, fighter_name, all_profile_data, fight_weightclass):
    # input: 1) a fighter name 2) a list of dictionaries from json file (fighter_profile) 3) weightclass of the fight
    # output: exactly * 1 * dictionary of a fighter profile
    
    # we init a list because there could be a situation where there are multiple entries of the same name (which is unwanted but a list is a safety check)
    profiles_found = []
    
    # loop through all profiles to check for matching names. If names, match, append profile dict to list. Return all results
    for profile_dict in all_profile_data:
      if profile_dict['name'] == fighter_name:
        profiles_found.append(profile_dict)
    
    # if only 1 profile found, return the profile
    if (len(profiles_found) == 1):
      return profiles_found[0]
    
    # if more than 1 profile found, check if their current weightclass matches the weightclass of the fight
    elif (len(profiles_found) > 1):
      print('sorting name conflict')
      new_list = []
      for profile in profiles_found:
        # get the weightclass as a number and convert it to text
        fighter_weightclass = self.weight_num_to_text(profile['weight'])
        # possible cases this breaks (when fight_weightclass is with 'W ' at the front / if fight_weightclass is 'catchweight' / if fighter changes weight)
        
        # print(f'fighter weightclass: {fighter_weightclass} || fight weightclass: {fight_weightclass}')
        if (fighter_weightclass == fight_weightclass):
          new_list.append(profile)
        # in case figher changes weight, we add / reduce 1 weight
        else:
          for weight in self.new_weights(fighter_weightclass):
            if (weight == fight_weightclass):
              new_list.append(profile)
              break
            
      # if after checking weightclass, there is 1 fighter left, return that profile. Else, return error
      if (len(new_list)== 1):
        return new_list[0]
      else:
        logger.error(f'Error getting fighter profile from json (multiple athletes with same name {fighter_name} + weightclass conflict')
        return None
    # if no athletes of the particular name found, return None
    else:
      logger.error(f'Error getting fighter profile from json (no fighter of name {fighter_name} found in json file')
      return None

  # Insert fighter in db ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  def insert_fighter(self, fighter, last_weightclass_fought):
    # input: a fighter dict from the profile list JSON file
    fighter_insert_query = "INSERT INTO fighters (name, dob, height, reach, last_weight_fought) VALUES (?, ?, ?, ?, ?);"
    fighter_insert_query_param = [fighter['name'], fighter['DOB'], fighter['height'], fighter['reach'], last_weightclass_fought]
    self.cursor.execute(fighter_insert_query, fighter_insert_query_param)
    return

  # Update fighter (weightclass) in db ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  def update_fighter(self, profile, last_weightclass_fought):
    # input: a fighter dict from the profile list JSON file, last weightclass fought
    
    # get fighter id using name + dob
    fighter_id = self.get_fighter_id_dob(profile['name'], profile['DOB'])
    
    query = "UPDATE fighters SET height = ?, reach = ?, last_weight_fought = ? WHERE id = ?;"
    param = [profile['height'], profile['reach'], last_weightclass_fought, fighter_id]
    self.cursor.execute(query, param)
    return

  # Insert ref in db ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  def insert_ref(self, ref_name):
    # input: a ref name
    ref_insert_query = "INSERT INTO referees (name) VALUES (?);"
    ref_insert_query_param = [ref_name]
    self.cursor.execute(ref_insert_query, ref_insert_query_param)
    return

  # Insert judge in db ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  def insert_judge(self, judge_name):
    # input: a judge name
    judge_insert_query = "INSERT INTO judges (name) VALUES (?);"
    judge_insert_query_param = [judge_name]
    self.cursor.execute(judge_insert_query, judge_insert_query_param)
    return

  # insert fighters, events, refs and judges ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  def check_insert_fighters_refs_judges(self, fight, fighter_profiles):
    # input: a fight dictionary, fighter_profiles_json list
    # output: boolean indicating whether update successful
    update_successful = True
    
    fight_weightclass = self.format_weightclass(fight['weightclass'])
    
    # loop twice for both fighters
    for i in range(1, 3):
      # get a figher profile dict from json (check done in function for multiple name conflict)
        fighter_profile_dict = self.get_fighter_dict_in_json_file(fight[f'fighter {i} name'], fighter_profiles, fight_weightclass)
        if (fighter_profile_dict != None):
          # check occurance of figher in db (by name and db)
          fighter_occurance_in_db = self.check_occurance_fighter(fighter_profile_dict['name'], fighter_profile_dict['DOB'])
          
          # if no occurance in db, insert into db
          if (fighter_occurance_in_db == 0):
            self.insert_fighter(fighter_profile_dict, fight_weightclass)
          # if *exactly 1* occurance, update stats
          elif (fighter_occurance_in_db == 1):
            self.update_fighter(fighter_profile_dict, fight_weightclass)
          else:
            logger.error('error. Multiple fighters with same name and DOB in database')
            return False
        else:
          logger.error('error. Multiple fighters with same name and weightclass found in json file')
          return False
          
    # check & insert ref
    if (self.check_occurance_ref(fight['referee']) == 0):
      self.insert_ref(fight['referee'])

    # check & insert judges. If fight went to scorecards.
    if ('scorecards' in fight.keys()):
      for key, value in fight['scorecards'].items():
        if ('name' in key):
          # check & insert judge
          if (self.check_occurance_judge(value) == 0):
            self.insert_judge(value)

    return update_successful

  # insert a tuple for fights ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  def insert_fight(self, event_id, fight):
    # input: event_id, a fight dictionary
    
    ref_id = self.get_ref_id(fight['referee'])
    weightclass = self.format_weightclass(fight['weightclass'])
    
    query = "INSERT INTO fights (event_id, fighter_1_name, fighter_2_name, fighter_1_result, fighter_2_result, weightclass, scheduled_rds, ending_rd, ending_time_min, ending_time_sec, win_method, referee_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"
    param = [event_id, fight['fighter 1 name'], fight['fighter 2 name'], fight['fighter 1 result'], fight['fighter 2 result'], weightclass, fight['scheduled rounds'], fight['ending round'], fight['ending time min'], fight['ending time sec'], fight['win method'], ref_id]
    self.cursor.execute(query, param)

  # insert judge scores ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  def insert_judge_scores(self, fight_id, scorecards):
    # input: fight id, scorecards dictionary
      for i in range(1, 4):
        # get judge id
        judge_id = self.get_judge_id(scorecards[f'judge {i} name'])
        query = "INSERT INTO judge_score (fight_id, judge_id, score_card) VALUES (?, ?, ?);"
        param = [fight_id, judge_id, scorecards[f'judge {i} score']]
        self.cursor.execute(query, param)

  # insert 1 fighter's fight stats (all rounds) ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  def insert_fight_stats_all_rounds(self, fighter_id, fight_id, list_rounds):
  # input: 1) a fighter id, 2) a fight id, 3) a list item corresponding to all rounds for 1 fighter for 1 bout

    for rd in list_rounds:
      query = "INSERT INTO fight_stats (fighter_id, fight_id, round, kd, sig_str, sig_str_att, sig_str_percent, total_str, total_str_att, total_str_percent, td, td_att, td_percent, sub_att, reversal, ctrl_time_min, ctrl_time_sec, sig_str_head, sig_str_head_att, sig_str_body, sig_str_body_att, sig_str_leg, sig_str_leg_att, sig_str_distance, sig_str_distance_att, sig_str_clinch, sig_str_clinch_att, sig_str_ground, sig_str_ground_att) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"
      param = [fighter_id, fight_id, rd['round'], rd['kd'], rd['sig str'], rd['sig str att'], rd['sig str percent'], rd['total str'], rd['total str att'], rd['total str percent'], rd['td'], rd['td att'], rd['td percent'], rd['sub att'], rd['reversal'], rd['ctrl time min'], rd['ctrl time sec'], rd['sig str head'], rd['sig str head att'], rd['sig str body'], rd['sig str body att'], rd['sig str leg'], rd['sig str leg att'], rd['sig str distance'], rd['sig str distance att'], rd['sig str clinch'], rd['sig str clinch att'], rd['sig str ground'], rd['sig str ground att']]
      self.cursor.execute(query, param)

  # insert or update fighter's stat ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  def insert_update_fighter_stats(self, fighter_id, fighter_profile):
    # input: a fighter's id, a profile dictionary (from json file)
    
    # check if tuple for fighter stats already exists
    exists_query = "SELECT COUNT(*) FROM fighter_stats WHERE fighter_id = ?;"
    exists_query_param = [fighter_id]
    stats_exists = self.cursor.execute(exists_query, exists_query_param).fetchone()[0]
    # additional params
    dic = {
      'num_ufc_fights': self.compute_num_ufc_fights(fighter_id),
      'num_total_fights': fighter_profile['win'] + fighter_profile['loss'] + fighter_profile['draw'] + fighter_profile['NC'],
      # win ratio excludes no contests
      'win_ratio': round(fighter_profile['win'] / (fighter_profile['win'] + fighter_profile['loss'] + fighter_profile['draw'])),
      'total_ufc_fight_time_sec': self.compute_total_ufc_fight_time(fighter_id),
      'avg_ufc_fight_time_sec': round(self.compute_total_ufc_fight_time(fighter_id) / self.compute_num_ufc_fights(fighter_id)),
      'sig_str_land': self.compute_total_sig_str_land(fighter_id),
      'sig_str_att': self.compute_sig_str_att(fighter_id),
      'td_land': self.compute_td_land(fighter_id),
      'td_att': self.compute_td_att(fighter_id),
      'kd': self.compute_kd(fighter_id)
    }
    if (stats_exists == 1):
      query = "UPDATE fighter_stats SET num_ufc_fights = ?, total_num_fights = ?, win_ratio = ?, win = ?, loss = ?, draw = ?, nc = ?, total_ufc_fight_time_sec = ?, avg_ufc_fight_time_sec = ?, sig_str_acc = ?, sig_str_land_permin = ?, sig_str_land = ?, sig_str_att = ?, kd = ?, sig_str_abs_permin = ?, sig_str_def = ?, td_acc = ?, td_land = ?, td_att = ?, td_land_avg_per15min = ?, sub_att_avg_per15min = ?, td_def = ? WHERE fighter_id = ?;"
      param = [dic['num_ufc_fights'], dic['num_total_fights'], dic['win_ratio'], fighter_profile['win'], fighter_profile['loss'], fighter_profile['draw'], fighter_profile['NC'], dic['total_ufc_fight_time_sec'], dic['avg_ufc_fight_time_sec'], fighter_profile['strike_acc'], fighter_profile['sig_str_land_permin'], dic['sig_str_land'], dic['sig_str_att'], dic['kd'], fighter_profile['sig_str_absorb_permin'], fighter_profile['sig_str_def'], fighter_profile['td_acc'], dic['td_land'], dic['td_att'], fighter_profile['td_avg_per15min'], fighter_profile['sub_avg_per15min'], fighter_profile['td_def'], fighter_id]
      self.cursor.execute(query, param)
    elif (stats_exists == 0):
      query = "INSERT INTO fighter_stats (fighter_id, num_ufc_fights, total_num_fights, win_ratio, win, loss, draw, nc, total_ufc_fight_time_sec, avg_ufc_fight_time_sec, sig_str_acc, sig_str_land_permin, sig_str_land, sig_str_att, kd, sig_str_abs_permin, sig_str_def, td_acc, td_land, td_att, td_land_avg_per15min, sub_att_avg_per15min, td_def) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"
      param = [fighter_id, dic['num_ufc_fights'], dic['num_total_fights'], dic['win_ratio'], fighter_profile['win'], fighter_profile['loss'], fighter_profile['draw'], fighter_profile['NC'], dic['total_ufc_fight_time_sec'], dic['avg_ufc_fight_time_sec'], fighter_profile['strike_acc'], fighter_profile['sig_str_land_permin'], dic['sig_str_land'], dic['sig_str_att'], dic['kd'], fighter_profile['sig_str_absorb_permin'], fighter_profile['sig_str_def'], fighter_profile['td_acc'], dic['td_land'], dic['td_att'], fighter_profile['td_avg_per15min'], fighter_profile['sub_avg_per15min'], fighter_profile['td_def']]
      self.cursor.execute(query, param)

  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # FUNCTIONS FOR COMPUTING EXTRA PARAMETERS FOR FIGHTER_STATS TABLE
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  
  # compute number of ufc fights ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  def compute_num_ufc_fights(self, fighter_id):
    # input: fighter's id
    # output: integer
    query = "SELECT COUNT(*) FROM fight_stats WHERE fighter_id = ? AND round = 0;"
    param = [fighter_id]
    total_ufc_fights = self.cursor.execute(query, param).fetchone()[0]
    return total_ufc_fights

  # compute total ufc fight time (seconds) ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  def compute_total_ufc_fight_time(self, fighter_id):
    # input: fighter's id
    # output: integer
    total_fight_time = 0
    
    # get all fight_id for a fighter
    query = "SELECT fight_id FROM fight_stats WHERE fighter_id = ? AND round = 0;"
    param = [fighter_id] 
    all_fight_id = [x[0] for x in self.cursor.execute(query, param).fetchall()]
    
    # loop through each fight
    for fight_id in all_fight_id:
      # make query
      query = "SELECT ending_rd, ending_time_min, ending_time_sec FROM fights WHERE id = ?;"
      param = [fight_id]
      result = self.cursor.execute(query, param).fetchall()[0]

      rounds, ending_time_min, ending_time_sec = result[0], result[1], result[2]
      
      # add final round to total time and subtract 1 to num rounds
      total_fight_time += ((ending_time_min * 60) + ending_time_sec)
      rounds -= 1
      
      # for each previous rounds, add 60 seconds
      while (rounds != 0):
        total_fight_time += 60
        rounds -= 1

    return total_fight_time

  def compute_total_sig_str_land(self, fighter_id):
    # input: fighter's id
    # output: integer
    query = "SELECT SUM(sig_str) FROM fight_stats WHERE fighter_id = ? AND round = 0;"
    param = [fighter_id]
    total_sig_str_land = self.cursor.execute(query, param).fetchone()[0]
    return total_sig_str_land

  # compute total sig strikes attempted in ufc fights ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  def compute_sig_str_att(self, fighter_id):
    # input: fighter's id
    # output: integer
    query = "SELECT SUM(sig_str_att) FROM fight_stats WHERE fighter_id = ? AND round = 0;"
    param = [fighter_id]
    total_sig_str_att = self.cursor.execute(query, param).fetchone()[0]
    return total_sig_str_att

  # compute total takedowns landed in ufc fights ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  def compute_td_land(self, fighter_id):
    # input: fighter's id
    # output: integer
    query = "SELECT SUM(td) FROM fight_stats WHERE fighter_id = ? AND round = 0;"
    param = [fighter_id]
    total_td_land = self.cursor.execute(query, param).fetchone()[0]
    return total_td_land

  # compute total takedown attempts in ufc fights ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  def compute_td_att(self, fighter_id):
    # input: fighter's id
    # output: integer
    query = "SELECT SUM(td_att) FROM fight_stats WHERE fighter_id = ? AND round = 0;"
    param = [fighter_id]
    total_td_att = self.cursor.execute(query, param).fetchone()[0]
    return total_td_att

  # compute total knockdowns scored in ufc fights ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  def compute_kd(self, fighter_id):
    # input: fighter's id
    # output: integer
    query = "SELECT SUM(kd) FROM fight_stats WHERE fighter_id = ? AND round = 0;"
    param = [fighter_id]
    total_kd = self.cursor.execute(query, param).fetchone()[0]
    return total_kd

  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # OTHER FUNCTIONS
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

  # Get weightclass change
  def new_weights(self, weight):
    # input: a weightclass text
    # output: a list of weightclass text
    output_list = []
    
    if (weight == 'Flyweight'):
      return ['Bantamweight']
    elif (weight == 'Bantamweight'):
      return ['Flyweight', 'Featherweight']
    elif (weight == 'Featherweight'):
      return ['Bantamweight', 'Lightweight']
    elif (weight == 'Lightweight'):
      return ['Featherweight', 'Welterweight']
    elif (weight == 'Welterweight'):
      return ['Lightweight', 'Middleweight']
    elif (weight == 'Middleweight'):
      return ['Welterweight', 'LightHeavyweight']
    elif (weight == 'LightHeavyweight'):
      return ['Middleweight', 'Heavyweight']
    elif (weight == 'Heavyweight'):
      return ['LightHeavyweight']
    else:
      return None

  # Convert weight from numbers to text (don't differentiate between m/f divs)
  def weight_num_to_text(self, weight_num):
    # input: a number
    # output: a text string
    if (weight_num == 115):
      return 'Strawweight'
    elif (weight_num == 125):
      return 'Flyweight'
    elif (weight_num == 135):
      return 'Bantamweight'
    elif (weight_num == 145):
      return 'Featherweight'
    elif (weight_num == 155):
      return 'Lightweight'
    # anything from 155 to 170
    elif ((weight_num > 155) and (weight_num <= 170)):
      return 'Welterweight'
    # anything from 171 to 185
    elif ((weight_num > 170) and (weight_num <= 185)):
      return 'Middleweight'
    # anything from 186 to 205
    elif ((weight_num > 185) and (weight_num <= 205)):
      return 'LightHeavyweight'
    # anything above 205
    elif (weight_num > 205):
      return 'Heavyweight'

  # formatting weightclass text ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  def format_weightclass(self, weightclass_text):
    # input: a text string
    # output: a text string
    output_string = ''
    if (('W ' in weightclass_text) or ('UFCW ' in weightclass_text)):
      output_string += 'W '
      
    if ('Strawweight' in weightclass_text):
      output_string += 'Strawweight'
    elif ('Flyweight' in weightclass_text):
      output_string += 'Flyweight'
    elif ('Bantamweight' in weightclass_text):
      output_string += 'Bantamweight'
    elif ('Featherweight' in weightclass_text):
      output_string += 'Featherweight'
    elif ('Lightweight' in weightclass_text):
      output_string += 'Lightweight'
    elif ('Welterweight' in weightclass_text):
      output_string += 'Welterweight'
    elif ('Middleweight' in weightclass_text):
      output_string += 'Middleweight'
    elif (re.search('LightHeavyweight' , weightclass_text)):
      output_string += 'LightHeavyweight'
    elif ((re.search('Heavyweight' , weightclass_text)) and (re.search('LightHeavyweight' , weightclass_text) == None)):
      output_string += 'Heavyweight'
    elif ('CatchWeight' in weightclass_text):
      output_string += 'CatchWeight'
    
    return output_string

  # del all fields from all tables in db ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  def delete_all_data_in_database(self):
    with closing(sqlite3.connect(self.database)) as db_connection:
      with closing(db_connection.cursor()) as cursor:
        queries = [
          "DELETE FROM events;", "DELETE FROM fighter_stats;", "DELETE FROM fights;", "DELETE FROM judges;",
          "DELETE FROM fight_stats;", "DELETE FROM fighters;", "DELETE FROM judge_score;", "DELETE FROM referees;"
          ]

        for query in queries:
          cursor.execute(query)
        
        # commit changes
        db_connection.commit()
        logger.info(f"Deleted data from all tables in database '{self.database}'")
  
  # reset database (drop all tables and create tables) ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  def reset_database(self):
    try:
      with closing(sqlite3.connect(self.database)) as db_connection:
        with closing(db_connection.cursor()) as cursor:
          
          queries_to_execute = []
          
          # open sql file - DROP table queries
          with open('scraper_files/sql_queries/drop_tables.sql', 'r') as fhandle:
            text_string = fhandle.read()
            # split queries, remove comments & whitespaces
            queries = text_string.split('\n')
            for query in queries:
              query.strip()
              if (('--' not in query) and ('*' not in query)):
                queries_to_execute.append(query)
          
          # open sql file - CREATE table queries
          with open('scraper_files/sql_queries/create_tables.sql', 'r') as fhandle:
            text_string = fhandle.read()
            # split queries, remove comments & whitespaces
            queries = text_string.split(';')
            for query in queries:
              query.strip()
              query += ";"
              if (('--' not in query) and ('*' not in query)):
                queries_to_execute.append(query)
            
          # execute all queries and commit changes
          for query in queries_to_execute:
            cursor.execute(query)
          db_connection.commit()
    except OperationalError:
      # print error message
      traceback.print_exc()
      logger.error(f"Error resetting database'{self.database}'")
    else:
      logger.info(f"Reset tables successful for database'{self.database}'")
  
  # Write 1 event to database~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  def write_1_event_to_database(self, event_indepth):
    # input: an indepth event dict
    # output: bool - update successful or not
    
    # if event is not complete...log and exit
    if (event_indepth['data completeness'] != 100):
      logger.error(f"Unable to update event. 'data completeness' is less than 100 for event: {event_indepth['event name']}")
      return False
    
    # If event is complete, check if event already in db 
    else:
      # If event already in db return false 
      if (self.check_occurance_event(event_indepth['event name'], event_indepth['date']) == 1):
        logger.error(f"Unable to update event. Event already in database.")
        return False
      
      # If event not in db, insert. (only insert if event occurance is 0)
      if (self.check_occurance_event(event_indepth['event name'], event_indepth['date']) == 0):
        # wrap everything in a try catch
        try:
          self.insert_event_in_db({'name': event_indepth['event name'], 'date': event_indepth['date'], 'country': event_indepth['country'], 'state': event_indepth['state']})
          
          # get event id
          event_id = self.get_event_id(event_indepth['event name'], event_indepth['date'])
          
          # get figher profiles (for updating and checking later)
          fighter_profiles = scraper_functions.read_json_data('scraper_files/scraped_data/fighter_profile.json')
          
          # create a list to store all the fights and reverse the order so it's updated in chronological order
          fights_list = []
          for fight in event_indepth['fights']:
            fights_list.append(fight)
          fights_list.reverse()
          
          # loop through each fight in event
          for fight in fights_list:
            # check & insert fighters, refs and judges (returns boolean for whether update is successful)
            if (self.check_insert_fighters_refs_judges(fight, fighter_profiles) == False):
              logger.error(f"Error at function 'check_insert_fighters_refs_judges'")
              return False

            # check & insert fight
            if (self.check_occurance_fight(event_id, fight['fighter 1 name'], fight['fighter 2 name']) == 0):   # EDIT CHECK USING FIGHTER ID!!!
              self.insert_fight(event_id, fight)

            # get newly made fight event id
            fight_id = self.get_fight_id(event_id, fight['fighter 1 name'], fight['fighter 2 name'])
            
            # if fight went to decision, input data for judge scores
            if (('scorecards' in fight.keys()) and (fight_id != None)):
              self.insert_judge_scores(fight_id, fight['scorecards'])
            
            fight_weightclass = self.format_weightclass(fight['weightclass'])
            
            # insert both fighters' fight stats (all rounds)
            for i in range(1, 3):
              fighter_id = self.get_fighter_id_fightweight(fight[f'fighter {i} name'], fight_weightclass)
              if (fighter_id != None):
                self.insert_fight_stats_all_rounds(fighter_id, fight_id, fight[f'fighter {i} stats'])
              else:
                logging.error("issue with function 'get_fighter_id'")
                return False
              
              # insert/update fighter stats.
              fighter_profile_dict = self.get_fighter_dict_in_json_file(fight[f'fighter {i} name'], fighter_profiles, fight_weightclass)
              if (fighter_profile_dict != None):
                self.insert_update_fighter_stats(fighter_id, fighter_profile_dict)
              else:
                logging.error("issue with function 'get_fighter_dict_in_json_file'")
                return False
                
        # If error, return false. (logging will be done outside of function)
        except OperationalError:
          # print error message
          traceback.print_exc()
          return False
      
        # Return true for update successful. (logging and commit will be done outside of function)
        else:
          return True

  # Write 1 event to db (with setup)
  def write_1_event_with_setup(self, event_indepth):
    # open database and cursor for executing queries
    with closing(sqlite3.connect(self.database)) as db_connection:
      with closing(db_connection.cursor()) as self.cursor:
        write_successful = self.write_1_event_to_database(event_indepth)

        # if write unsuccessful, log error and break loop
        if (write_successful == False):
          logger.error(f"Event: {event_indepth['event name']} - Update unsuccessful :<")
        
        # else if event successful, commit changes to database. log. (Update scraper date logs to date of LAST SUCCESSFUL EVENT UPDATED) 
        if (write_successful == True):
          db_connection.commit()
          logger.info(f"Event: {event_indepth['event name']} - Update successful :>")

  # get list of events to update to database ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  def get_list_events_to_update(self):
    # output: list of basic event dicts from 'past_events.json'
    
    # init a list to write to db
    list_events_to_store_in_db = []
    
    # check scraper date logs for when last updated to database
    
    # get the date of last update 
    last_update = scraper_functions.check_field_scraper_date_logs('update_database')
    last_update = datetime.strptime(last_update, '%d/%b/%Y %H:%M:%S')

    # read past events basic info from json
    file_path_past_events = 'scraper_files/scraped_data/past_events.json'
    past_events_basic_info_dicts = scraper_functions.read_json_data(file_path_past_events)

    # loop through past events (basic data)
    for event in past_events_basic_info_dicts:
      # if the date of the event is after / same as last updated date...
      if (datetime.strptime(event['date'], '%d/%b/%Y') >= last_update):
        # check to see if the event has already been written to database
        event_occ_in_db = self.check_occurance_event(event['event name'], event['date'])

        # if event name not found in database
        if (event_occ_in_db == 0):
          # make a dict and store data
          nw_dict = dict()
          nw_dict['name'] = event['event name']
          nw_dict['date'] = event['date']
          nw_dict['year'] = event['date'].split('/')[2]
          list_events_to_store_in_db.append(nw_dict)
          
      # if date of event is before last updated date, break loop 
      else:
        break
    
    # reverse the list so it is in chronological order
    list_events_to_store_in_db.reverse()
    return list_events_to_store_in_db

  # get indepth event dict ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  def get_indepth_event_dict(self, event_basic): 
    # input: a dict - basic event details
    # output: a dict - indepth event details / None

    # get the names of all files for each year of fights
    folder_path = os.getcwd() + r"scraper_files\scraped_data\all_fights_by_year"

    # read file data for that year
    file_path = r'scraper_files\scraped_data\all_fights_by_year'
    file_path += f'\{event_basic["year"]}_events.json'
    all_events_for_year = scraper_functions.read_json_data(file_path)

    # loop through the events for that year and return the dict with the matching name and date
    for event in all_events_for_year:
      # if there is file for the year of this event...
      if ((event['event name'] == event_basic['name']) and (event['date'] == event_basic['date'])):
        return event
    
    # if after looping through all, if no event match, return None. Logging of error will be done outside of function
    return None

  # UPDATE DATABASE ~ MOST IMPORTANT FUNCTION! ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  def update_db(self):
  # return True if successful. return False if unsuccessful
    try:
      # open database and cursor for executing queries
      with closing(sqlite3.connect(self.database)) as db_connection:
        with closing(db_connection.cursor()) as self.cursor:
          
          # get list of basic dicts to update
          events_to_update = self.get_list_events_to_update()

          if (len(events_to_update) == 0):
            logger.info('No events to update (database)')
            return True
          
          else:
            # init var for num events update successful + var to get date of latest event updated in db
            num_successfully_updated = 0
            date_most_recent_event_updated = ''
            
            # loop through each event
            for event_basic_dict in events_to_update:
              # get indepth dict and write to database
              indepth_event_dict = self.get_indepth_event_dict(event_basic_dict)
              write_successful = self.write_1_event_to_database(indepth_event_dict)

              # if write unsuccessful, log error and break loop
              if (write_successful == False):
                logger.error(f"Event: {event_basic_dict['name']} - Update unsuccessful :<")
                break
              
              # else if event successful, commit changes to database. log. (Update scraper date logs to date of LAST SUCCESSFUL EVENT UPDATED) 
              if (write_successful == True):
                db_connection.commit()
                logger.info(f"Event: {event_basic_dict['name']} - Update successful :>")
                num_successfully_updated += 1
                date_most_recent_event_updated = event_basic_dict['date']
 
            # update time in scraper date logs
            scraper_functions.update_scraper_date_logs('update_database', date_most_recent_event_updated)
            logger.info(f"Number of events updated to database: {num_successfully_updated}")
            return True

    except OperationalError:
      # print error message
      traceback.print_exc()
      logger.error("Error with 'update_db' function")
      return False
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def main():
  updater = db_updater('ufc_db.db')
  #updater.reset_database()
  update_status = updater.update_db()
  if (update_status == True):
    print('update successful')
  else:
    print('update unsuccessful')
if __name__ == "__main__":
  main()