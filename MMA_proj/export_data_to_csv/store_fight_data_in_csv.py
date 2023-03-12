import sqlite3; from sqlite3 import OperationalError; import traceback
from datetime import datetime
from contextlib import closing
import re; import json; import csv
# connect to database
import os
# get filepath to database
parent_dir = os.path.dirname(os.getcwd())
db_filepath = parent_dir + r'\ufc_db.db'

'''
The algorithm is that for each fight...
  for each fighter,
    -get the date of all of that fighter's fights before that fight.
    -get all the metrics
    -add as a row data in csv file
'''
class data_compiler():
  def __init__(self, database_file_path_name):
    self.database = database_file_path_name
    self.cursor = None
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# SUPPORTER FUNCS (needed for other functions)

  # Normal divide function Returns Number or None
  def divide_func(self, numerator, denominator):
    if (denominator in [0, None]):
      return None
    else:
      return (numerator / denominator)

  # function to return a rounded percentage or None 
  def calc_percent(self, numerator, denominator):
    if (denominator == 0):
      return None
    else:
      # return accuracy as rounded integer
      return round((numerator / denominator) * 100)

  # function to read json to get desired fight metrics
  def whole_list_from_json_file(self):
    # input: takes file path as argument
    # output: list
    with open('metrics_to_use.json', 'r', encoding="utf-8") as file_handler:
      data = json.load(file_handler)
      #print(json.dumps(data, indent=4, ensure_ascii=False))
      return data

  def desired_metrics_names(self):
    # output: list
    
    # get whole list data. Init a return list as well
    json_list = self.whole_list_from_json_file()
    return_list = []
    
    # loop through all data and if inidicated 1, store in return list
    for i in range(0, len(json_list), 2):
      if (json_list[i+1] == 1):
        return_list.append(json_list[i])
    
    return return_list
  
  def get_all_date_and_fight_id_for_a_fighter(self, fighter_id):
    # input: fighter id
    # output: a list of fights (fight_date, fight_id) for 1 fighter
    query = "SELECT events.date, fights.id FROM events JOIN fights ON events.id = fights.event_id WHERE fights.id IN (SELECT fight_id FROM fight_stats WHERE fighter_id = ? AND round = 0);"
    param = [fighter_id]
    return self.cursor.execute(query, param).fetchall()

  def all_fights_of_fighter_before_date(self, fighter_id, date):
    # input: fighter id + fight date
    # output: a list of fight ids before a given date (of a fight) / None
    
    # format time to datetime obj
    f_date = datetime.strptime(date, '%d/%b/%Y')
    
    return_list = []
    
    # get all of fighter's fights. Loop through. If fight is before input date, add it to return list
    for fight in self.get_all_date_and_fight_id_for_a_fighter(fighter_id):
      if (datetime.strptime(fight[0], '%d/%b/%Y') < f_date):
        return_list.append(fight[1])

    if (len(return_list) == 0):
      return_list = None

    return return_list

  def get_last_n_fights_before_date(self, fighter_id, date, last_n_fights):
    # This function returns the most recent n amount of fights (id) of a fighter before a date, in chronological order.
    # If fighter does not have that amount of fights, return None
    # input: num of most recent fights to return
    # output: a list of fight id / None
    
    # get all fights of fighter
    all_fights = self.all_fights_of_fighter_before_date(fighter_id, date)
    
    if (all_fights == None) or (last_n_fights < 1):
      return None
    elif (len(all_fights) < last_n_fights):
      return None
    elif (len(all_fights) == last_n_fights):
      return all_fights
    else:
      return_list = []
      counter = 0
      index = len(all_fights)-last_n_fights
      while counter != last_n_fights:
        return_list.append(all_fights[index])
        counter +=1
        index += 1
      return return_list

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# FUNCTIONS THAT RETURN A METRIC
# all funcs here are about getting a fighter's stat BEFORE A GIVEN (FIGHT) DATE.
# 'compile_all_metrics' will call all the funcs except the ones that are 'pending' 

  def get_fighter_name(self, fighter_id):
    query = "SELECT name FROM fighters WHERE id = ?;"
    param = [fighter_id]
    return self.cursor.execute(query, param).fetchone()[0]

  def get_age(self, fighter_id, date):
    # get bday of fighter
    query = "SELECT dob FROM fighters WHERE id = ?;"
    param = [fighter_id]
    bday = self.cursor.execute(query, param).fetchone()[0]
    
    # if fighter's have no bday recorded (in db), return None
    if (bday == None):
      return None
    else:
      # convert bday, fight date to datetime obj. 
      bday = datetime.strptime(bday, '%d/%b/%Y')
      date_compared = datetime.strptime(date, '%d/%b/%Y')
      
      # calc num of days between both dates
      num_days = (date_compared - bday).days
      # convert to year, round to 1dp
      age_years_1_dp = round((num_days/365), 1)
      
      return age_years_1_dp

  def get_days_since_last_fight(self, fighter_id, date):
    # output: integer (number of days) / None
    
    # get last 1 fight (func returns a list)
    last_fight_id = self.get_last_n_fights_before_date(fighter_id, date, 1)
    
    # if no previous fight recorded, this will be None
    if (last_fight_id == None):
      return None
    # if last fight recorded...
    else:
      # get date of last fight
      query = "SELECT events.date FROM events JOIN fights ON events.id = fights.event_id where fights.id = ?"
      param = [last_fight_id[0]]
      last_fight_date_result = self.cursor.execute(query, param).fetchone()[0]

      # convert fight date, last fight date to datetime objs
      fight_date = datetime.strptime(date, '%d/%b/%Y')
      last_fight_date = datetime.strptime(last_fight_date_result, '%d/%b/%Y')
      
      # calc and return num of days between both dates
      return (fight_date - last_fight_date).days

  def get_record_before_date(self, fighter_id, date):
    # output: a dictionary
    # algorithm: get fighter's current record and subtract all records on and after this (fight) date 
    
    # embedded function 1
    def get_curr_record(fighter_id):
      query = "SELECT win, loss, draw, nc FROM fighter_stats WHERE fighter_id = ?;"
      param = [fighter_id]
      record = self.cursor.execute(query, param).fetchone()
      return record[0], record[1], record[2], record[3]

    # embedded function 2
    def all_fights_of_fighter_on_or_after_date(fighter_id, date):
      # format time to datetime obj
      f_date = datetime.strptime(date, '%d/%b/%Y')
      
      return_list = []
      
      # get all of fighter's fights. Loop through. If fight is before input date, add it to return list
      for fight in self.get_all_date_and_fight_id_for_a_fighter(fighter_id):
        if (datetime.strptime(fight[0], '%d/%b/%Y') >= f_date):
          return_list.append(fight[1])

      if (len(return_list) == 0):
        return_list = None

      return return_list

    # MAIN FUNCTION ~~~~~~~~~
    # get fighter's name + current record
    fighter_name = self.get_fighter_name(fighter_id)
    win_curr, loss_curr, draw_curr, nc_curr = get_curr_record(fighter_id)

    # init vars to count numbers to subtract 
    win_to_subtract, loss_to_subtract, draw_to_subtract, nc_to_subtract = 0, 0, 0, 0
    query = "SELECT fighter_1_name, fighter_2_name, fighter_1_result, fighter_2_result FROM fights WHERE id = ?;"
    
    # get all of fighter's fights on and after input date. Loop through.
    all_fights_on_or_after_date = all_fights_of_fighter_on_or_after_date(fighter_id, date)
    
    # if no fights, return current record
    if (all_fights_on_or_after_date == None):
      win_rate = round(win_curr / (win_curr + loss_curr) * 100)
      return {'win_rate': win_rate, 'num_win': win_curr, 'num_loss': loss_curr, 'num_draw': draw_curr, 'num_nc': nc_curr}
    else:
      for fight_id in all_fights_on_or_after_date:
        # get results of the fight
        param = [fight_id]
        fight_details = self.cursor.execute(query, param).fetchone()
        f1_name, f2_name, f1_result, f2_result = fight_details[0], fight_details[1], fight_details[2], fight_details[3]
        
        # check name of fighter and get fight result
        result = None
        if (fighter_name == f1_name):
          result = f1_result
        elif (fighter_name == f2_name):
          result = f2_result

        # check fight result and increment vars to subtract accordingly
        match result:
          case 'W':
            win_to_subtract += 1
          case 'L':
            loss_to_subtract += 1
          case 'D':
            draw_to_subtract += 1
          case 'NC':
            nc_to_subtract += 1

      # subtract records to get records before a fight
      win_curr -= win_to_subtract
      loss_curr -= loss_to_subtract
      draw_curr -= draw_to_subtract
      nc_curr -= nc_to_subtract
      if ((win_curr + loss_curr) == 0):
        win_rate = None
      else:
        win_rate = round(win_curr / (win_curr + loss_curr) * 100)

      return {'win_rate': win_rate, 'num_win': win_curr, 'num_loss': loss_curr, 'num_draw': draw_curr, 'num_nc': nc_curr}

  def get_ufc_record_and_fight_outcomes(self, fighter_id, date, last_n_fights=None):
    # algorithm: get all fighter's fights check all results before given fight date
    
    # get fighter's name
    fighter_name = self.get_fighter_name(fighter_id)
    
    # init stuff to return (ufc record dict) + (fight outcomes dict)
    ufc_rec_dict_and_fight_outcomes = {
      'win_rate_ufc': None, 'num_win_ufc': 0, 'num_loss_ufc': 0, 'num_draw_ufc': 0, 'num_nc_ufc': 0,

      'finish_win_percent': None, 'num_finish_win': 0,
      'ko_win_percent': None, 'num_ko_win': 0,
      'sub_win_percent': None, 'num_sub_win': 0,
      'ud_win_percent': None, 'num_ud_win': 0,
      
      'finish_loss_percent': None, 'num_finish_loss': 0,
      'ko_loss_percent': None, 'num_ko_loss': 0,
      'sub_loss_percent': None, 'num_sub_loss': 0,
      'ud_loss_percent': None, 'num_ud_loss': 0
    }

    # get fight id list
    if (last_n_fights==None):
      fight_id_list = self.all_fights_of_fighter_before_date(fighter_id, date)
    else:
      fight_id_list = self.get_last_n_fights_before_date(fighter_id, date, last_n_fights)
    
    # if fight id list is None, convert all fields to None and return
    if (fight_id_list == None):
      for field in ufc_rec_dict_and_fight_outcomes.keys():
        ufc_rec_dict_and_fight_outcomes[field] = None
      return ufc_rec_dict_and_fight_outcomes

    # else if not none, go through the main function
    else:
      # Loop through fight_id_list
      for fight_id in fight_id_list:
        # get results of the fight
        query = "SELECT fighter_1_name, fighter_2_name, fighter_1_result, fighter_2_result, win_method FROM fights WHERE id = ?;"
        param = [fight_id]
        fight_details = self.cursor.execute(query, param).fetchone()
        f1_name, f2_name, f1_result, f2_result, method = fight_details[0], fight_details[1], fight_details[2], fight_details[3], fight_details[4]
        
        # check name of fighter and get fight result
        result = None
        if (fighter_name == f1_name):
          result = f1_result
        elif (fighter_name == f2_name):
          result = f2_result

        # check fight result and increment vars accordingly
        match result:
            case 'W':
              ufc_rec_dict_and_fight_outcomes['num_win_ufc'] += 1
              # increment win methods
              if (method in ['KO/TKO', "TKO - Doctor'sStoppage"]):
                ufc_rec_dict_and_fight_outcomes['num_finish_win'] += 1
                ufc_rec_dict_and_fight_outcomes['num_ko_win'] += 1
              elif (method == 'Submission'):
                ufc_rec_dict_and_fight_outcomes['num_finish_win'] += 1
                ufc_rec_dict_and_fight_outcomes['num_sub_win'] += 1
              elif (method == 'Decision - Unanimous'):
                ufc_rec_dict_and_fight_outcomes['num_ud_win'] += 1

            case 'L':
              ufc_rec_dict_and_fight_outcomes['num_loss_ufc'] += 1
              # increment loss methods
              if (method in ['KO/TKO', "TKO - Doctor'sStoppage"]):
                ufc_rec_dict_and_fight_outcomes['num_finish_loss'] += 1
                ufc_rec_dict_and_fight_outcomes['num_ko_loss'] += 1
              elif (method == 'Submission'):
                ufc_rec_dict_and_fight_outcomes['num_finish_loss'] += 1
                ufc_rec_dict_and_fight_outcomes['num_sub_loss'] += 1
              elif (method == 'Decision - Unanimous'):
                ufc_rec_dict_and_fight_outcomes['num_ud_loss'] += 1
            case 'D':
              ufc_rec_dict_and_fight_outcomes['num_draw_ufc'] += 1
            case 'NC':
              ufc_rec_dict_and_fight_outcomes['num_nc_ufc'] += 1

      # calc all percentages
      # win rate excludes draws / no contests
      ufc_rec_dict_and_fight_outcomes['win_rate_ufc'] = self.calc_percent(ufc_rec_dict_and_fight_outcomes['num_win_ufc'], (ufc_rec_dict_and_fight_outcomes['num_win_ufc'] + ufc_rec_dict_and_fight_outcomes['num_loss_ufc']))
      ufc_rec_dict_and_fight_outcomes['finish_win_percent'] = self.calc_percent(ufc_rec_dict_and_fight_outcomes['num_finish_win'], ufc_rec_dict_and_fight_outcomes['num_win_ufc'])
      ufc_rec_dict_and_fight_outcomes['ko_win_percent'] = self.calc_percent(ufc_rec_dict_and_fight_outcomes['num_ko_win'], ufc_rec_dict_and_fight_outcomes['num_win_ufc'])
      ufc_rec_dict_and_fight_outcomes['sub_win_percent'] = self.calc_percent(ufc_rec_dict_and_fight_outcomes['num_sub_win'], ufc_rec_dict_and_fight_outcomes['num_win_ufc'])
      ufc_rec_dict_and_fight_outcomes['ud_win_percent'] = self.calc_percent(ufc_rec_dict_and_fight_outcomes['num_ud_win'], ufc_rec_dict_and_fight_outcomes['num_win_ufc'])
      ufc_rec_dict_and_fight_outcomes['finish_loss_percent'] = self.calc_percent(ufc_rec_dict_and_fight_outcomes['num_finish_loss'], ufc_rec_dict_and_fight_outcomes['num_loss_ufc'])
      ufc_rec_dict_and_fight_outcomes['ko_loss_percent'] = self.calc_percent(ufc_rec_dict_and_fight_outcomes['num_ko_loss'], ufc_rec_dict_and_fight_outcomes['num_loss_ufc'])
      ufc_rec_dict_and_fight_outcomes['sub_loss_percent'] = self.calc_percent(ufc_rec_dict_and_fight_outcomes['num_sub_loss'], ufc_rec_dict_and_fight_outcomes['num_loss_ufc'])
      ufc_rec_dict_and_fight_outcomes['ud_loss_percent'] = self.calc_percent(ufc_rec_dict_and_fight_outcomes['num_ud_loss'], ufc_rec_dict_and_fight_outcomes['num_loss_ufc'])
      
      return ufc_rec_dict_and_fight_outcomes

  def get_total_and_avg_ufc_fight_time(self, fighter_id, date, last_n_fights=None):
    # output: A dictionary with 2 integers (total fight time, avg fight time) corresponding to minutes(rounded)
    
    # init var for number of fights, fight time (mins), fight time (seconds)
    total_num_fights, total_fight_mins, total_fight_secs = 0, 0, 0
    
    # get fight id list
    if (last_n_fights==None):
      fight_id_list = self.all_fights_of_fighter_before_date(fighter_id, date)
    else:
      fight_id_list = self.get_last_n_fights_before_date(fighter_id, date, last_n_fights)

    # if fight_id_list is None
    if (fight_id_list == None):
      return {'total_ufc_fight_time': None, 'avg_ufc_fight_time': None}
    else:
      # Loop through.
      for fight_id in fight_id_list:
        # increment num_fights
        total_num_fights += 1

        # get ending_rd, ending_time_min, ending_time_sec
        query = "SELECT ending_rd, ending_time_min, ending_time_sec FROM fights WHERE id = ?;"
        param = [fight_id]
        fight_details = self.cursor.execute(query, param).fetchone()
        end_rd, fight_mins, fight_secs  = fight_details[0], fight_details[1], fight_details[2]
        
        # compute total time for fight
        total_fight_mins += (end_rd - 1) * 5
        total_fight_mins += fight_mins
        total_fight_secs += fight_secs

      # conv secs to mins (rounded) then add to total mins
      total_fight_mins += round(total_fight_secs/60)
      
      # calc avg fight time
      avg_fight_time_mins = round(total_fight_mins/total_num_fights)
      
      
      
      # return
      return {'total_ufc_fight_time': total_fight_mins, 'avg_ufc_fight_time': avg_fight_time_mins}
      
  def get_striking_and_grappling_metrics(self, fighter_id, date, last_n_fights=None):
    # input: optional n number of fights to get data from
    # output: 1 dictionary containing all striking and grappling metrics 

    # init dictionaries for striking metrics and grappling metrics
    metrics_dict = {
      'kd': 0,
      'sig_str_acc': None, 'sig_str_land_permin': 0, 'sig_str_land': 0, 'sig_str_att': 0,
      'total_str_acc': None, 'total_str_land': 0, 'total_str_att': 0,
      # striking def
      'sig_str_def_acc': None, 'sig_str_abs_permin': 0, 'sig_str_opp_land': 0, 'sig_str_opp_att': 0,
      # striking by target
      'sig_str_head_acc': None, 'sig_str_head_land': 0, 'sig_str_head_att': 0,
      'sig_str_body_acc': None, 'sig_str_body_land': 0, 'sig_str_body_att': 0,
      'sig_str_leg_acc': None, 'sig_str_leg_land': 0, 'sig_str_leg_att': 0,
      # striking by position
      'sig_str_distance_acc': None, 'sig_str_distance_land': 0, 'sig_str_distance_att': 0,
      'sig_str_clinch_acc': None, 'sig_str_clinch_land': 0, 'sig_str_clinch_att': 0,
      'sig_str_ground_acc': None, 'sig_str_ground_land': 0, 'sig_str_ground_att': 0,
      # grappling stuff
      'td_acc': None, 'td_land_permin': 0, 'td_land': 0, 'td_att': 0, 
      'ctrl_time': 0, 'sub_att_permin': 0, 'sub_att': 0,
      # grappling def
      'td_def_acc': None, 'td_opp_land': 0, 'td_opp_att': 0,
      'ctrled_time': 0, 'reversal': 0
    }
    
    # vars to use later
    ctrl_time_s, ctrled_time_s = 0, 0
    
    # get fight id list
    if (last_n_fights==None):
      fight_id_list = self.all_fights_of_fighter_before_date(fighter_id, date)
    else:
      fight_id_list = self.get_last_n_fights_before_date(fighter_id, date, last_n_fights)
    
    # if get_last_n_fights but is equal to none, return a dictionary with all fields = None
    if (fight_id_list == None):
      for field in metrics_dict.keys():
        metrics_dict[field] = None
      return metrics_dict
    else:
      # get all of fighter's fights. Loop through. If fight is before input date...
      for fight_id in fight_id_list:
        # query db and get fight stats
        query = "SELECT * FROM fight_stats WHERE fighter_id = ? AND fight_id = ? AND round = 0;"
        param = [fighter_id, fight_id]
        fight_stats = self.cursor.execute(query, param).fetchone()
        
        # increment all stats
        # striking stats overall
        metrics_dict['kd'] += fight_stats[3]
        metrics_dict['sig_str_land'] += fight_stats[4];     metrics_dict['sig_str_att'] += fight_stats[5]
        metrics_dict['total_str_land'] += fight_stats[7];   metrics_dict['total_str_att'] += fight_stats[8]
        # strikes by target
        metrics_dict['sig_str_head_land'] += fight_stats[17];   metrics_dict['sig_str_head_att'] += fight_stats[18]
        metrics_dict['sig_str_body_land'] += fight_stats[19];   metrics_dict['sig_str_body_att'] += fight_stats[20]
        metrics_dict['sig_str_leg_land'] += fight_stats[21];    metrics_dict['sig_str_leg_att'] += fight_stats[22]
        # strikes by position
        metrics_dict['sig_str_distance_land'] += fight_stats[23];   metrics_dict['sig_str_distance_att'] += fight_stats[24]
        metrics_dict['sig_str_clinch_land'] += fight_stats[25];     metrics_dict['sig_str_clinch_att'] += fight_stats[26]
        metrics_dict['sig_str_ground_land'] += fight_stats[27];     metrics_dict['sig_str_ground_att'] += fight_stats[28]
        # grappling stats overall
        metrics_dict['td_land'] += fight_stats[10];   metrics_dict['td_att'] += fight_stats[11]
        metrics_dict['ctrl_time'] += fight_stats[15]
        ctrl_time_s += fight_stats[16]
        metrics_dict['sub_att'] += fight_stats[13]
        metrics_dict['reversal'] += fight_stats[14]


        # query db and get opponent fight stats
        query = "SELECT * FROM fight_stats WHERE fighter_id != ? AND fight_id = ? AND round = 0;"
        param = [fighter_id, fight_id]
        opp_fight_stats = self.cursor.execute(query, param).fetchone()
        
        # add stats to dict
        metrics_dict['sig_str_opp_land'] += opp_fight_stats[4];   metrics_dict['sig_str_opp_att'] += opp_fight_stats[5]
        metrics_dict['td_opp_land'] += opp_fight_stats[10];       metrics_dict['td_opp_att'] += opp_fight_stats[11]
        metrics_dict['ctrled_time'] += opp_fight_stats[15]
        ctrled_time_s += fight_stats[16]

      # compute all accuracy stats
      metrics_dict['sig_str_acc'] = self.calc_percent(metrics_dict['sig_str_land'], metrics_dict['sig_str_att'])
      metrics_dict['total_str_acc'] = self.calc_percent(metrics_dict['total_str_land'], metrics_dict['total_str_att'])
      metrics_dict['sig_str_def_acc'] = self.calc_percent(metrics_dict['sig_str_opp_land'], metrics_dict['sig_str_opp_att'])
      metrics_dict['sig_str_head_acc'] = self.calc_percent(metrics_dict['sig_str_head_land'], metrics_dict['sig_str_head_att'])
      metrics_dict['sig_str_body_acc'] = self.calc_percent(metrics_dict['sig_str_body_land'], metrics_dict['sig_str_body_att'])
      metrics_dict['sig_str_leg_acc'] = self.calc_percent(metrics_dict['sig_str_leg_land'], metrics_dict['sig_str_leg_att'])
      metrics_dict['sig_str_distance_acc'] = self.calc_percent(metrics_dict['sig_str_distance_land'], metrics_dict['sig_str_distance_att'])
      metrics_dict['sig_str_clinch_acc'] = self.calc_percent(metrics_dict['sig_str_clinch_land'], metrics_dict['sig_str_clinch_att'])
      metrics_dict['sig_str_ground_acc'] = self.calc_percent(metrics_dict['sig_str_ground_land'], metrics_dict['sig_str_ground_att'])
      metrics_dict['td_acc'] = self.calc_percent(metrics_dict['td_land'], metrics_dict['td_att'])
      metrics_dict['td_def_acc'] = self.calc_percent(metrics_dict['td_opp_land'], metrics_dict['td_opp_att'])
      
      # compute permin stats but first, get fighter's total fight time
      total_fight_time = self.get_total_and_avg_ufc_fight_time(fighter_id, date)['total_ufc_fight_time']
      
      metrics_dict['sig_str_land_permin'] = self.divide_func(metrics_dict['sig_str_land'], total_fight_time)
      metrics_dict['sig_str_abs_permin'] = self.divide_func(metrics_dict['sig_str_opp_land'], total_fight_time)
      metrics_dict['td_land_permin'] = self.divide_func(metrics_dict['td_land_permin'], total_fight_time)
      metrics_dict['sub_att_permin'] = self.divide_func(metrics_dict['sub_att'], total_fight_time)
      
      # compute control time and control'd time
      metrics_dict['ctrl_time'] += round(ctrl_time_s/60)
      metrics_dict['ctrled_time'] += round(ctrled_time_s/60)

      return metrics_dict

  # opponent's combined record, combined UFC records, combined UFC finishing rate 
  def get_opponent_combined_record(self, fighter_id, date, last_n_opponents=None):
    # get opponent's combined record and finishes

    # init a dictionary to hold data
    opp_combi_rec = {
      'opp_win_rate': None, 'opp_num_win': 0, 'opp_num_loss': 0,
      'opp_win_rate_ufc': None, 'opp_num_win_ufc': 0, 'opp_num_loss_ufc': 0,
      'opp_finish_win_percent': None, 'opp_num_finish_win': 0
      }
    
    # get fight id list
    if (last_n_opponents==None):
      fight_id_list = self.all_fights_of_fighter_before_date(fighter_id, date)
    else:
      fight_id_list = self.get_last_n_fights_before_date(fighter_id, date, last_n_opponents)

    # if fight_id_list is None
    if (fight_id_list == None):
      for field in opp_combi_rec.keys():
        opp_combi_rec[field] = None
      return opp_combi_rec
    else:
      # get all of fighter's fights. Loop through.
      for fight_id in fight_id_list:
        # for each fight, query db to get the opponent's id
        query = "SELECT fighter_id FROM events JOIN fights ON events.id = fights.event_id JOIN fight_stats ON fights.id = fight_stats.fight_id WHERE fighter_id != ? AND fight_id = ? AND round = 0;"
        param = [fighter_id, fight_id]
        opp_id = self.cursor.execute(query, param).fetchone()[0]
        
        # get the opponent's record, ufc rec, ufc outcomes 
        opp_rec = self.get_record_before_date(opp_id, date)
        opp_ufc_rec_and_outcomes = self.get_ufc_record_and_fight_outcomes(opp_id, date)
        
        # add values to the dict to return
        opp_combi_rec['opp_num_win'] += opp_rec['num_win']
        opp_combi_rec['opp_num_loss'] += opp_rec['num_loss']
        opp_combi_rec['opp_num_win_ufc'] += opp_ufc_rec_and_outcomes['num_win_ufc']
        opp_combi_rec['opp_num_loss_ufc'] += opp_ufc_rec_and_outcomes['num_loss_ufc']
        opp_combi_rec['opp_num_finish_win'] += opp_ufc_rec_and_outcomes['num_finish_win']

      # compute all percentages
      opp_combi_rec['opp_win_rate'] = self.calc_percent(opp_combi_rec['opp_num_win'], (opp_combi_rec['opp_num_win'] + opp_combi_rec['opp_num_loss']))
      opp_combi_rec['opp_win_rate_ufc'] = self.calc_percent(opp_combi_rec['opp_num_win_ufc'], (opp_combi_rec['opp_num_win_ufc'] + opp_combi_rec['opp_num_loss_ufc']))
      opp_combi_rec['opp_finish_win_percent'] = self.calc_percent(opp_combi_rec['opp_num_finish_win'], opp_combi_rec['opp_num_win_ufc'])

    return opp_combi_rec

  # Get last n fight metrics & consistency metrics (consistency implies last n fights)
  def get_last_n_fight_metrics(self, fighter_id, date):
    # 2 embedded functions are necessary for the main function to work
    # embedded function 1
    def get_num_last_fights(targeted_names_list):
      # input: 1) a list correponding to all the metric names in the json file
      # output: a list with numbers. Possible that it will return an empty list '[]'
      
      # get all metrics names from json and init a list to hold all metric names
      desired_metrics_frm_json = self.whole_list_from_json_file()
      return_list = []
      
      # loop through all metric names from json file (skip 1s and 0s) and store in 'metric_names' list
      for i in range(0, len(desired_metrics_frm_json), 2):
        # if any of the names have 'l_[0-9]_' (use regex), and if they are desired (1 is indicated)...
        if (len(re.findall('l_[0-9]+_', desired_metrics_frm_json[i])) == 1) and (desired_metrics_frm_json[i+1]==1):
          # get 5th char to the end
          string_to_chk = desired_metrics_frm_json[i][4:]
          # check if that string appears in 'targeted_names_list'
          if (string_to_chk in targeted_names_list):
            num = int(re.findall('_[0-9]+_', desired_metrics_frm_json[i])[0].replace('_', '').replace('l', ''))
            # if is not in return list, append
            if (num not in return_list):
              return_list.append(num)

      return return_list

    # embedded function 2
    def change_key_names(a_dictionary, last_n_num):
      # input: a dictionary with KVPs, a number corresponding to the number of last fights for the data
      # output: a dictionary, with the key names being edited
      
      # init a return dict
      return_dict = dict()
      
      # format start of name
      last_n = 'l_' + str(last_n_num) + '_'
      
      # loop through all dict keys, add 'l_n_' to the front of every key and assign the same value
      for key in a_dictionary.keys():
        nw_key = last_n + key
        return_dict[nw_key] = a_dictionary[key]
      
      return return_dict

    # MAIN FUNC ~~~~~~~~~~~~~~
    # init a return dict
    return_dict = dict()

    # algorithm:
    # 1) we make a dictionary. Each key key contains a list of target metrics. The target metrics are checked to see if there's a last_n desired for that metric.
    # 2) we loop throuh each dictionary key and for each, there's a different function associated that will be called according

    dict_of_target_list = {
      'ufc records and fight outcomes from last n fights':  ['win_rate_ufc', 'num_win_ufc', 'num_loss_ufc', 'num_draw_ufc', 'num_nc_ufc', 'finish_win_percent', 'num_finish_win', 'ko_win_percent', 'num_ko_win', 'sub_win_percent', 'num_sub_win', 'ud_win_percent', 'num_ud_win', 'finish_loss_percent', 'num_finish_loss', 'ko_loss_percent', 'num_ko_loss', 'sub_loss_percent', 'num_sub_loss', 'ud_loss_percent', 'num_ud_loss'],
      'striking and grappling metrics from last n fights':  ['kd', 'sig_str_acc', 'sig_str_land_permin', 'sig_str_land', 'sig_str_att', 'total_str_acc', 'total_str_land', 'total_str_att', 'sig_str_def_acc', 'sig_str_abs_permin', 'sig_str_opp_land', 'sig_str_opp_att', 'sig_str_head_acc', 'sig_str_head_land', 'sig_str_head_att', 'sig_str_body_acc', 'sig_str_body_land', 'sig_str_body_att', 'sig_str_leg_acc', 'sig_str_leg_land', 'sig_str_leg_att', 'sig_str_distance_acc', 'sig_str_distance_land', 'sig_str_distance_att', 'sig_str_clinch_acc', 'sig_str_clinch_land', 'sig_str_clinch_att', 'sig_str_ground_acc', 'sig_str_ground_land', 'sig_str_ground_att', 'td_acc', 'td_land_permin', 'td_land', 'td_att', 'ctrl_time', 'sub_att_permin', 'sub_att', 'td_def_acc', 'td_opp_land', 'td_opp_att', 'ctrled_time', 'reversal'],
      'opp combined rec from last n fights':                ["opp_win_rate", "opp_win", "opp_loss", "opp_win_rate_ufc", "opp_win_ufc", "opp_loss_ufc", "opp_finish_win_percent", "opp_finish_win"],
      'total and avg fight time from last n fights':        ["total_ufc_fight_time", "avg_ufc_fight_time"]
    }
    
    # loop through the dict of target list
    for key in dict_of_target_list.keys():
      # get the last_n for that target list set (gets back a list of ints. possible empty list)
      last_n = get_num_last_fights(dict_of_target_list[key])
      
      # loop though list
      for num_last_n in last_n:
        # check key name and use associated function accordingly
        match key:
          case 'ufc records and fight outcomes from last n fights':
            data_dict = self.get_ufc_record_and_fight_outcomes(fighter_id, date, num_last_n)
          case 'striking and grappling metrics from last n fights':
            data_dict = self.get_striking_and_grappling_metrics(fighter_id, date, num_last_n)
          case 'opp combined rec from last n fights':
            data_dict = self.get_opponent_combined_record(fighter_id, date, num_last_n)
          case 'total and avg fight time from last n fights':
            data_dict = self.get_total_and_avg_ufc_fight_time(fighter_id, date, last_n)

        # change key names. Then add to return dict
        nw_dict_w_changed_key_names = change_key_names(data_dict, num_last_n)
        return_dict.update(nw_dict_w_changed_key_names)

    return return_dict

  def get_consistency_metrics(self, fighter_id, date):
    # output: a dictionary
    
    # SUPPORT FUNCTIONS ~~~~~~~~~~~~~~~~~~~~~
    # look at desired metrics json file and get the last n of all consistency metrics
    def get_num_last_fights():
      # This function reads desired metrics from json and checks to see if a metric has '_cons_' and returns the number corresponding to the 'last n'
      # input: nothing
      # output: a list with numbers. Possible that it will return an empty list '[]'
      
      # get all metrics names from json and init a list to hold all metric names
      desired_metrics_frm_json = self.whole_list_from_json_file()
      return_list = []
      
      # loop through all metric names from json file (skip 1s and 0s) and store in 'metric_names' list
      for i in range(0, len(desired_metrics_frm_json), 2):
        # if any of the names have 'l_[0-9]_' and '_cons_' (use regex), and if they are desired (1 is indicated)...
        if (len(re.findall('l_[0-9]+_.+_cons_', desired_metrics_frm_json[i])) == 1) and (desired_metrics_frm_json[i+1]==1):
          num = int(re.findall('_[0-9]+_', desired_metrics_frm_json[i])[0].replace('_', '').replace('l', ''))
          # if is not in return list, append
          if (num not in return_list):
            return_list.append(num)

      return return_list

    def change_key_names(a_dictionary, last_n_num):
      # input: a dictionary with KVPs, a number corresponding to the number of last fights for the data
      # output: a dictionary, with the key names being edited
      
      # init a return dict
      chg_name_rtn_dict = dict()
      
      # format start of name
      last_n = 'l_' + str(last_n_num) + '_'
      
      # loop through all dict keys, add 'l_n_' to the front of every key and assign the same value
      for key in a_dictionary.keys():
        nw_key = last_n + key
        chg_name_rtn_dict[nw_key] = a_dictionary[key]
      
      return chg_name_rtn_dict
    
    # MAIN FUNCTION ~~~~~~~~~~~~~~~~~~~~~
    # Init a return dict
    return_dict = dict()
    
    # get list of nums for last n fights
    last_n_fights_list = get_num_last_fights()
    
    for last_n in last_n_fights_list:
      # create a new metric dict
      cons_metrics_dict = {
        'sig_str_acc_cons_1':         0,  'sig_str_acc_cons_2':          1,
        'sig_str_land_per10s_cons_1': 0,  'sig_str_land_per10s_cons_2':  1,
        'td_acc_cons_1':              0,  'td_acc_cons_2':               1,
        'td_land_permin_cons_1':      0,  'td_land_permin_cons_2':       1
      }

      # init a dictionary to hold the data from each fight (for use to calc consistency metrics)
      data_list = {
        'sig_str_acc': [],   'sig_str_land_per10s': [],
        'td_acc': [],        'td_land_permin': [],
      }
      
      # get n last fight ids before the fight date
      fight_list = self.get_last_n_fights_before_date(fighter_id, date, last_n)

      # if fight_list is None
      if (fight_list == None):
        nw_dict = change_key_names(cons_metrics_dict, last_n)
        # set all fields to None
        for name in nw_dict.keys():
          nw_dict[name] = None
        # add to main return dict
        return_dict.update(nw_dict)
      else:
        # query each fight and get fight stats to store in data list (for use to calc consistency metrics)
        for fight_id in fight_list:
          # query db and get fight time and fight stats
          query = "SELECT ending_rd, ending_time_min, ending_time_sec, sig_str_percent, sig_str, td_percent, td FROM fights JOIN fight_stats ON fights.id = fight_stats.fight_id WHERE fighter_id = ? AND fight_id = ? AND round = 0;"
          param = [fighter_id, fight_id]
          fight_stats = self.cursor.execute(query, param).fetchone()
          end_rd, end_time_min, end_time_sec, sig_str_percent, sig_str, td_percent, td = fight_stats[0], fight_stats[1], fight_stats[2], fight_stats[3], fight_stats[4], fight_stats[5], fight_stats[6]

          # calc total time for fight (total seconds and total rounded minutes)
          total_fight_secs = ((((end_rd - 1) * 5) + end_time_min) * 60) + end_time_sec
          fight_mins = ((end_rd - 1) * 5) + end_time_min + round(end_time_sec / 60)

          # append accuracy stats. compute and append permin stats
          data_list['sig_str_acc'].append(sig_str_percent)
          data_list['td_acc'].append(td_percent)
          data_list['sig_str_land_per10s'].append((sig_str/total_fight_secs) * 10)

          # exception for fast finishes (under 30s), we just make td_land_permin to be = 0
          if (fight_mins == 0):
            data_list['td_land_permin'].append(0)
          else:
            data_list['td_land_permin'].append(td / fight_mins)

        # loop through each data point and calc for consistency metric 1 and 2 
        for i in range (last_n - 1):
          # calc all cons metric 1. We do this by summing the differences between each data point
          
          # sig_str_acc_cons_1 (if any data point in None, the cons metric is none)
          if (None in data_list['sig_str_acc']):
            cons_metrics_dict['sig_str_acc_cons_1'] = None
          else:
            cons_metrics_dict['sig_str_acc_cons_1'] += (data_list['sig_str_acc'][i+1] - data_list['sig_str_acc'][i])
          # sig_str_land_permin_cons_1
          cons_metrics_dict['sig_str_land_per10s_cons_1'] += (data_list['sig_str_land_per10s'][i+1] - data_list['sig_str_land_per10s'][i])
          # td_acc_cons_1 (if any data point in None, the cons metric is none)
          if (None in data_list['td_acc']):
            cons_metrics_dict['td_acc_cons_1'] = None
          else:
            cons_metrics_dict['td_acc_cons_1'] += (data_list['td_acc'][i+1] - data_list['td_acc'][i])
          # td_land_permin_cons_1
          cons_metrics_dict['td_land_permin_cons_1'] += (data_list['td_land_permin'][i+1] - data_list['td_land_permin'][i])
          
          # calc all cons metric 2. We do this by first setting the value at 1 (already done when init dict). If the value is zero, we don't check it.
          # (if the value is still 1) and (if the next data point is smaller than the previous, we change the value to zero.
          
          # sig_str_acc_cons_2
          if (None in data_list['sig_str_acc']):
            cons_metrics_dict['sig_str_acc_cons_2'] = 0
          else:
            if (cons_metrics_dict['sig_str_acc_cons_2'] != 0) and (data_list['sig_str_acc'][i+1] < data_list['sig_str_acc'][i]):
              cons_metrics_dict['sig_str_acc_cons_2'] = 0
          # calc for sig_str_land_permin_cons_2
          if (cons_metrics_dict['sig_str_land_per10s_cons_2'] != 0) and (data_list['sig_str_land_per10s'][i+1] < data_list['sig_str_land_per10s'][i]):
            cons_metrics_dict['sig_str_land_per10s_cons_2'] = 0
          # calc for td_acc_cons_2
          if (None in data_list['td_acc']):
            cons_metrics_dict['td_acc_cons_2'] = 0
          else:
            if (cons_metrics_dict['td_acc_cons_2'] != 0) and (data_list['td_acc'][i+1] < data_list['td_acc'][i]):
              cons_metrics_dict['td_acc_cons_2'] = 0
          # calc for td_land_permin_cons_2
          if (cons_metrics_dict['td_land_permin_cons_2'] != 0) and (data_list['td_land_permin'][i+1] < data_list['td_land_permin'][i]):
            cons_metrics_dict['td_land_permin_cons_2'] = 0

        # change metric names to add 'l_(0-9)_' & add to main return dict
        dict_w_nw_names = change_key_names(cons_metrics_dict, last_n)
        return_dict.update(dict_w_nw_names)

    return return_dict

  # PENDING FUNCTIONS ~~~~~~~~
  def power_metrics (self):
  # for last .... fights, 
    # using all records, get power ratio
    # total KOs / sig strikes
    # total KOs / fights    
    return None
  
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# SUPPORT FUNCTIONS
  def get_all_fights(self):
    query = "SELECT fights.id, events.date, fighter_1_name, fighter_2_name, fighter_1_result, fighter_2_result, weightclass, scheduled_rds, ending_rd, ending_time_min, ending_time_sec, win_method FROM events JOIN fights ON events.id = fights.event_id;"
    return self.cursor.execute(query).fetchall()

  def get_fighters_fight_data_and_profile_info(self, fight_id):
    query = "SELECT * FROM fighters JOIN fight_stats ON fighters.id = fight_stats.fighter_id WHERE fight_id = ? AND round = 0;"
    param = [fight_id]
    return self.cursor.execute(query, param).fetchall()

  # Function that combines 1) compiling metrics, 2) match selecting from json list
  def compile_metrics_for_one_fighter(self, fighter_id, fight_date):
    # outputs: a dictionary
    
    def compile_metrics(fighter_id, fight_date):
      # this function gets all the metrics (as of input fight date) and combine them into 1 dictionary (any N.A. fields should have value None)
      # input: 1) fighter_id 2) fight id, 3) fight date
      # output: a dictionary 

      # init return dict
      return_dict = dict()
      
      # get fighter name & age (rounded to 1 dp)
      return_dict['name'] = self.get_fighter_name(fighter_id)
      return_dict['age'] = self.get_age(fighter_id, fight_date)

      # get days since last fight
      return_dict['days_since_last_fight'] = self.get_days_since_last_fight(fighter_id, fight_date)

      # get total & average UFC fight times (both in rounded minutes)
      return_dict.update(self.get_total_and_avg_ufc_fight_time(fighter_id, fight_date))
      
      # get overall record of fighter before this fight
      return_dict.update(self.get_record_before_date(fighter_id, fight_date))
      
      # get ufc record before this fight & fight outcomes
      return_dict.update(self.get_ufc_record_and_fight_outcomes(fighter_id, fight_date))

      # get striking and grappling metrics
      return_dict.update(self.get_striking_and_grappling_metrics(fighter_id, fight_date))

      # get opp combined record
      return_dict.update(self.get_opponent_combined_record(fighter_id, fight_date))

      # get last n fight metrics and consistency metrics (if any) 
      return_dict.update(self.get_last_n_fight_metrics(fighter_id, fight_date))
      return_dict.update(self.get_consistency_metrics(fighter_id, fight_date))
      
      return return_dict

    def match_slect_to_json_list(input_dictionary):
      # this function checks all key names in input dictionary and cross references it to json list.
      
      # init a return dict
      return_dict = dict()
      
      # read desired metrics from json list
      desired_metrics = self.desired_metrics_names()
      
      # loop through every key in input dictionary
      for key_name in input_dictionary.keys():
        # if key name in desired metrics list...
        if (key_name in desired_metrics):
          return_dict[key_name] = input_dictionary[key_name]

      return return_dict
    
    # putting it together
    compile = compile_metrics(fighter_id, fight_date)
    final_dict = match_slect_to_json_list(compile)
    
    return final_dict

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 2 MAIN COMPILER FUNCTIONSSUPPORT FUNCTIONS
  # Compiler 1 -- sets each row of data to include both fighters. Total rows of data = total num of fights
  def compile_to_csv_1(self, csv_file_name = None):
    try:
      # open database and cursor for executing queries
      with closing(sqlite3.connect(self.database)) as db_connection:
        with closing(db_connection.cursor()) as self.cursor:
      
          # select all fight data
          list_of_fights = self.get_all_fights()
          
          # read desired metrics from json list
          desired_metrics = self.desired_metrics_names()
          
          # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
          # create headers for the list ~~~~~~~~~~
          headers = ['date', 'fight_id', 'weightclass']
          
          # Adding all the f1 and f2 metrics by first making a 2 loop
          for i in range (1, 3):
            # set either 'f1_' or 'f2_'
            f1_or_f2 = 'f' + str(i) + '_'
            for metric_name in desired_metrics:
              nw_header_name = f1_or_f2 + metric_name
              headers.append(nw_header_name)
            
            # add their fight result
            headers.append(f1_or_f2 + 'result')
          
          # add win method
          headers.append('win_method')

          # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
          # Init a list to store all lists of data
          all_data = []
          
          # Loop through each fight ~~~~~~~~~~~~~~
          for fight in list_of_fights:
            fight_id, fight_date, win_method, weightclass, fighter_1_name, fighter_1_result, fighter_2_name, fighter_2_result = fight[0], fight[1], fight[11], fight[6], fight[2], fight[4], fight[3], fight[5]
            
            #@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
            if (fight_id in [5, 10, 25, 50, 100, 200, 500, 1000, 1500, 2000, 2500, 3000, 3500, 4000, 4500, 5000, 5500, 6000, 6500]):
              print(fight_id)
            #print('fight_id = ' + str(fight_id))
            
            # select the 2 tuples corresponding to each fighter's fight stats + their information
            both_fighters_fight_data = self.get_fighters_fight_data_and_profile_info(fight_id)

            # create a list and store 1) date 2) fight id 
            fight_data_list = [fight_date, fight_id, weightclass]

            # loop for both fighters...
            for fighter in both_fighters_fight_data:

              # get the fighter's id
              fighter_id = fighter[0]
              #print('fighter_id = ' + str(fighter_id)) #@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@

              # compile the stats for the fighter
              compiled_stats = self.compile_metrics_for_one_fighter(fighter_id, fight_date)
              
              # loop through desired metric names and append the fighter's data to the list
              for metric in self.desired_metrics_names():
                fight_data_list.append(compiled_stats[metric])
          
              # Write their fight result
              if (compiled_stats['name'] == fighter_1_name):
                fight_data_list.append(fighter_1_result)
              else:
                fight_data_list.append(fighter_2_result)
            
            # After writing both fighter's data, write win method
            fight_data_list.append(win_method)
            
            # append data to giant list for all fights
            all_data.append(fight_data_list)

          # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
          # Write to CSV
          # check if file name is None
          if (csv_file_name == None):
            csv_file_name = 'compiled_fight_data_type_1.csv' 
            
          with open(csv_file_name, 'w+', newline='', encoding='utf8') as f_handle:
            writer = csv.writer(f_handle, delimiter=',')
        
            # write headers
            writer.writerow(headers)
            
            # write multiple rows of data
            writer.writerows(all_data)

    except OperationalError:
      # print error message
      traceback.print_exc()

  # Compiler 2 -- This function splits each fight into 2 rows of data, 1 for each fighter. Total rows of data = num of fights * 2
  def compile_to_csv_2(self, csv_file_name = None):
    try:
      # open database and cursor for executing queries
      with closing(sqlite3.connect(self.database)) as db_connection:
        with closing(db_connection.cursor()) as self.cursor:
          # select all fight data
          list_of_fights = self.get_all_fights()
          
          # read desired metrics from json list
          desired_metrics = self.desired_metrics_names()
          
          # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
          # create headers for the list ~~~~~~~~~~
          headers = ['date', 'fight_id', 'weightclass']
          
          # Add all json metric names 
          for metric_name in desired_metrics:
            headers.append(metric_name)
          
          # add their fight result
          headers.append('result')
          
          # add win method
          headers.append('win_method')

          # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
          # Init a list to store all lists of data
          all_data = []
          
          # Loop through each fight ~~~~~~~~~~~~~~
          for fight in list_of_fights:
            fight_id, fight_date, win_method, weightclass, fighter_1_name, fighter_1_result, fighter_2_name, fighter_2_result = fight[0], fight[1], fight[11], fight[6], fight[2], fight[4], fight[3], fight[5]
            
            #@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
            if (fight_id in [5, 10, 25, 50, 100, 200, 500, 1000, 1500, 2000, 2500, 3000, 3500, 4000, 4500, 5000, 5500, 6000, 6500]):
              print(fight_id)
            #print('fight_id = ' + str(fight_id))
            
            # select the 2 tuples corresponding to each fighter's fight stats + their information
            both_fighters_fight_data = self.get_fighters_fight_data_and_profile_info(fight_id)

            # loop for both fighters...
            for fighter in both_fighters_fight_data:
              
              # create a list and store 1) date 2) fight id 
              fight_data_list = [fight_date, fight_id, weightclass]

              # get the fighter's id
              fighter_id = fighter[0]

              # compile the stats for the fighter
              compiled_stats = self.compile_metrics_for_one_fighter(fighter_id, fight_date)

              # loop through desired metric names and append the fighter's data to the list
              for metric in self.desired_metrics_names():
                fight_data_list.append(compiled_stats[metric])
          
              # Write their fight result
              if (compiled_stats['name'] == fighter_1_name):
                fight_data_list.append(fighter_1_result)
              else:
                fight_data_list.append(fighter_2_result)
            
              # Write win method
              fight_data_list.append(win_method)
            
              # append data to giant list for all fights
              all_data.append(fight_data_list)

          # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
          # Write to CSV
          # check if file name is None
          if (csv_file_name == None):
            csv_file_name = 'compiled_fight_data_type_2.csv'
            
          with open(csv_file_name, 'w+', newline='', encoding='utf8') as f_handle:
            writer = csv.writer(f_handle, delimiter=',')
        
            # write headers
            writer.writerow(headers)
            
            # write multiple rows of data
            writer.writerows(all_data)
    except OperationalError:
      # print error message
      traceback.print_exc()

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def main():
  print(datetime.now())
  compiler = data_compiler(db_filepath)
  compiler.compile_to_csv_2()
  print(datetime.now())
 
if __name__ == "__main__":
  main()