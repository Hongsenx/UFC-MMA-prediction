from bs4 import BeautifulSoup


# TAKE_NOTE: This script is a part of the 'scrape_events.py' script. The scrape_events.py script is the main script.

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
class Get_fight_details:
  
  def __init__(self, soup):
    self.soup = soup
  
  # GET NAMES + RESULTS ~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  def get_fighter_names(self):
    # init a list
    fighter_names = []
    # get 2 tags    
    names_tag = self.soup.find_all('h3', class_='b-fight-details__person-name')
    
    # get text value. strip spaces. add to list
    for name in names_tag:
      fighter_names.append(name.get_text().strip())
      
    # return list w 2 items. eg. ['Karol Rosa', 'Lina Lansberg']
    return fighter_names 
    
  def get_fighter_1_name(self):
    return self.get_fighter_names()[0]
  
  def get_fighter_2_name(self):
    return self.get_fighter_names()[1]

  # ~~~~~~
  def get_fighters_results(self):
    # init a list
    fighters_results = []
    # get 2 tags    
    results_tags = self.soup.find_all('i', class_='b-fight-details__person-status')
    
    # get text value. strip spaces. add to list
    for result in results_tags:
      fighters_results.append(result.get_text().strip())
      
    # return list w 2 results eg. ['W', 'L']
    return fighters_results

  def get_fighter_1_result(self):
    return self.get_fighters_results()[0]
  
  def get_fighter_2_result(self):
    return self.get_fighters_results()[1]

  # GET BOUT DETAILS ~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  def get_weightclass(self):
    weightclass = self.soup.find('div', 'b-fight-details__fight-head').get_text().strip().replace(" ", "").replace("Women's", "W ").replace("Bout", "")
    return weightclass
  
  def get_some_bout_details(self):
    # init a dict
    bout_details = dict()
    
    # get win method
    win_method = self.soup.find('i', class_='b-fight-details__text-item_first').get_text().replace(' ', '').replace('\n', '').split('Method:')[1].replace('-', ' - ')
    bout_details['win method'] = win_method
    
    # get all other stuff
    first_content_div = self.soup.find('div', 'b-fight-details__content').find('p', class_='b-fight-details__text').find_all('i', class_='b-fight-details__text-item')
    
    # get ending round (typecast to int)
    ending_round = first_content_div[0].get_text().strip().replace(' ', '').replace('\n', '').split('Round:')[1]
    bout_details['ending round'] = int(ending_round)
    
    # get ending time (minute & seconds) (typecast to int)
    ending_time = first_content_div[1].get_text().strip().replace(' ', '').replace('\n', '').split('Time:')[1].split(':')   
    bout_details['ending time min'] = int(ending_time[0])
    bout_details['ending time sec'] = int(ending_time[1])
    
    # get scheduled rounds (typecast to int)
    scheduled_rounds = first_content_div[2].get_text().strip().replace(' ', '').replace('\n', '').split(':')[1][0:1]
    bout_details['scheduled rounds'] = int(scheduled_rounds)
    
    # get referee
    referee = first_content_div[3].find('span').get_text().strip()
    bout_details['referee'] = referee
    
  
    # get finish details and/or scorecard ~~~~~
    second_content_div = self.soup.find('div', 'b-fight-details__content').find_all('p', class_='b-fight-details__text')[1]
    
    # get scorecard tags
    scorecard_tags = second_content_div.find_all('i', class_='b-fight-details__text-item')

    # if there are no scorecards (implies a finish)
    if (len(scorecard_tags) == 0):
      # get finish details
      finish_details = second_content_div.get_text().strip().replace('\n', '').split('Details:')[1].strip()
      bout_details['finish details'] = finish_details
    
    # if there exists scorecard tags...
    if (len(scorecard_tags)):
      bout_details['finish details'] = 'scorecards'
      
      # init a new dict for judge
      bout_details['scorecards'] = dict()
      
      # loop through all judges
      for i in range(len(scorecard_tags)):
        counter = i + 1
        
        # judge name
        name = scorecard_tags[i].find('span').get_text()
        
        # get judge score
        score = scorecard_tags[i].get_text().replace('\n', '').replace(' ', '').replace('.', '') 
        score = score.replace(name.replace(' ', ''), '')
        
        # append details 
        bout_details['scorecards']['judge '+ str(counter) + ' name'] = name
        bout_details['scorecards']['judge '+ str(counter) + ' score'] = score   
    
    return bout_details

  # GET TOTALS OVERVIEW + BY ROUND DETAILS ~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  def data_model_totals(self):
      return {
        "kd": None,
        "sig str": None,
        "sig str att": None,
        "sig str percent": None,
        "total str": None,
        "total str att": None,
        "total str percent": None,
        "td": None,
        "td att": None,
        "td percent": None,
        "sub att": None,
        "reversal": None,
        "ctrl time min": None,
        "ctrl time sec": None 
      }
  
  def get_totals(self, row_data):
    # init list to store 2 dictionaries. init data model dicts
    totals_for_both_fighters = []
    fighter1_totals = self.data_model_totals()
    fighter2_totals = self.data_model_totals()
    
    # get 10 columns
    columns = row_data.find_all('td', class_='b-fight-details__table-col')
    
    # GET NAME    TO DELETE!!!!!
    #names = columns[0].select('p')
    #fighter1_totals['name'] = names[0].get_text().strip()
    #fighter2_totals['name'] = names[1].get_text().strip()
    
    # GET KNOCKDOWNS
    knockdowns = columns[1].select('p')
    fighter1_totals['kd'] = int(knockdowns[0].get_text().strip())
    fighter2_totals['kd'] = int(knockdowns[1].get_text().strip())
    
    # GET SIG STRIKES AND SIG STRIKE ATTEMPTS
    sig_str_and_att = columns[2].select('p')
    fighter1_totals['sig str'] = int(sig_str_and_att[0].get_text().strip().replace(" ", "").split("of")[0])
    fighter1_totals['sig str att'] = int(sig_str_and_att[0].get_text().strip().replace(" ", "").split("of")[1])
    
    fighter2_totals['sig str'] = int(sig_str_and_att[1].get_text().strip().replace(" ", "").split("of")[0])
    fighter2_totals['sig str att'] = int(sig_str_and_att[1].get_text().strip().replace(" ", "").split("of")[1])
        
    # COMPUTE SIG STRIKES PERCENT (possible for null values)
    if (fighter1_totals['sig str att'] == 0):
      fighter1_totals['sig str percent'] = None
    else:
      fighter1_totals['sig str percent'] = round((fighter1_totals['sig str'] / fighter1_totals['sig str att'])*100)
      
    if (fighter2_totals['sig str att'] == 0):
      fighter2_totals['sig str percent'] = None
    else:
      fighter2_totals['sig str percent'] = round((fighter2_totals['sig str'] / fighter2_totals['sig str att']) * 100)
    
    # GET TOTAL STRIKES AND TOTAL STRIKE ATTEMPTS
    total_str_and_att = columns[4].select('p')
    fighter1_totals['total str'] = int(total_str_and_att[0].get_text().strip().replace(" ", "").split("of")[0])
    fighter1_totals['total str att'] = int(total_str_and_att[0].get_text().strip().replace(" ", "").split("of")[1])
    
    fighter2_totals['total str'] = int(total_str_and_att[1].get_text().strip().replace(" ", "").split("of")[0])
    fighter2_totals['total str att'] = int(total_str_and_att[1].get_text().strip().replace(" ", "").split("of")[1])
        
    # COMPUTE TOTAL STRIKES PERCENT (POSSIBLE FOR NULL VALUES)
    if (fighter1_totals['total str att'] == 0):
      fighter1_totals['total str percent'] = None
    else:
      fighter1_totals['total str percent'] = round((fighter1_totals['total str'] / fighter1_totals['total str att'])*100)
      
    if (fighter2_totals['total str att'] == 0):
      fighter2_totals['total str percent'] = None
    else:
      fighter2_totals['total str percent'] = round((fighter2_totals['total str'] / fighter2_totals['total str att']) * 100)
    
    # GET TAKEDOWN AND ATTEMPTS
    td_and_att = columns[5].select('p')
    fighter1_totals['td'] = int(td_and_att[0].get_text().strip().replace(" ", "").split("of")[0])
    fighter1_totals['td att'] = int(td_and_att[0].get_text().strip().replace(" ", "").split("of")[1])
    
    fighter2_totals['td'] = int(td_and_att[1].get_text().strip().replace(" ", "").split("of")[0])
    fighter2_totals['td att'] = int(td_and_att[1].get_text().strip().replace(" ", "").split("of")[1])

    # COMPUTE TAKEDOWN PERCENT (POSSIBLE FOR NULL VALUES)
    if (fighter1_totals['td att'] == 0):
      fighter1_totals['td percent'] = None
    else:
      fighter1_totals['td percent'] = round((fighter1_totals['td'] / fighter1_totals['td att'])*100)
      
    if (fighter2_totals['td att'] == 0):
      fighter2_totals['td percent'] = None
    else:
      fighter2_totals['td percent'] = round((fighter2_totals['td'] / fighter2_totals['td att']) * 100)
    
    # GET SUBMISSION ATTEMPTS
    sub_att = columns[7].select('p')
    fighter1_totals['sub att'] = int(sub_att[0].get_text().strip())
    fighter2_totals['sub att'] = int(sub_att[1].get_text().strip())
    
    # GET REVERSALS
    reversal = columns[8].select('p')
    fighter1_totals['reversal'] = int(reversal[0].get_text().strip())
    fighter2_totals['reversal'] = int(reversal[1].get_text().strip())
    
    # GET CONTROL TIME
    ctrl_time = columns[9].select('p')
    time_f1 = ctrl_time[0].get_text().strip().replace(" ", "").split(":")
    time_f2 = ctrl_time[1].get_text().strip().replace(" ", "").split(":")
    
    fighter1_totals['ctrl time min'] = int(time_f1[0]) 
    fighter1_totals['ctrl time sec'] = int(time_f1[1])
    fighter2_totals['ctrl time min'] = int(time_f2[0])
    fighter2_totals['ctrl time sec'] = int(time_f2[1])

    # ~~~~~~~~~~~~~~~~~~~~~~~~~
    # APPEND BOTH FIGHTER STATS TO LIST
    totals_for_both_fighters.append(fighter1_totals)
    totals_for_both_fighters.append(fighter2_totals)
    
    return totals_for_both_fighters

  # GET SIGNIFICANT STRIKES OVERVIEW + BY ROUND DETAILS ~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  def data_model_sig_strikes(self):
      return {
        "sig str head": None,
        "sig str head att": None,
        "sig str body": None,
        "sig str body att": None,
        "sig str leg": None,
        "sig str leg att": None,
        "sig str distance": None,
        "sig str distance att": None,
        "sig str clinch": None,
        "sig str clinch att": None,
        "sig str ground": None,
        "sig str ground att": None
      }
  
  def get_sig_strikes(self, row_data):
    # init list to store 2 dictionaries. init data model dicts
    sig_strikes_both_fighters = []
    fighter1_sig_str = self.data_model_sig_strikes()
    fighter2_sig_str = self.data_model_sig_strikes()
    
    # get 9 columns
    columns = row_data.find_all('td', class_='b-fight-details__table-col')
    
    # first 3 columns are useless, we start at column 4 (index 3)
  
    # GET SIG STRIKES *HEAD* + ATTEMPTS
    head_strikes = columns[3].select('p')
    fighter1_sig_str['sig str head'] = int(head_strikes[0].get_text().strip().replace(" ", "").split("of")[0])
    fighter1_sig_str['sig str head att'] = int(head_strikes[0].get_text().strip().replace(" ", "").split("of")[1])
    
    fighter2_sig_str['sig str head'] = int(head_strikes[1].get_text().strip().replace(" ", "").split("of")[0])
    fighter2_sig_str['sig str head att'] = int(head_strikes[1].get_text().strip().replace(" ", "").split("of")[1])
    
    # GET SIG STRIKES *BODY* + ATTEMPTS
    body_strikes = columns[4].select('p')
    fighter1_sig_str['sig str body'] = int(body_strikes[0].get_text().strip().replace(" ", "").split("of")[0])
    fighter1_sig_str['sig str body att'] = int(body_strikes[0].get_text().strip().replace(" ", "").split("of")[1])
    
    fighter2_sig_str['sig str body'] = int(body_strikes[1].get_text().strip().replace(" ", "").split("of")[0])
    fighter2_sig_str['sig str body att'] = int(body_strikes[1].get_text().strip().replace(" ", "").split("of")[1])
    
    # GET SIG STRIKES *LEG* + ATTEMPTS
    leg_strikes = columns[5].select('p')
    fighter1_sig_str['sig str leg'] = int(leg_strikes[0].get_text().strip().replace(" ", "").split("of")[0])
    fighter1_sig_str['sig str leg att'] = int(leg_strikes[0].get_text().strip().replace(" ", "").split("of")[1])
    
    fighter2_sig_str['sig str leg'] = int(leg_strikes[1].get_text().strip().replace(" ", "").split("of")[0])
    fighter2_sig_str['sig str leg att'] = int(leg_strikes[1].get_text().strip().replace(" ", "").split("of")[1])
    
    # GET SIG STRIKES *DISTANCE* + ATTEMPTS
    distance_strikes = columns[6].select('p')
    fighter1_sig_str['sig str distance'] = int(distance_strikes[0].get_text().strip().replace(" ", "").split("of")[0])
    fighter1_sig_str['sig str distance att'] = int(distance_strikes[0].get_text().strip().replace(" ", "").split("of")[1])
    
    fighter2_sig_str['sig str distance'] = int(distance_strikes[1].get_text().strip().replace(" ", "").split("of")[0])
    fighter2_sig_str['sig str distance att'] = int(distance_strikes[1].get_text().strip().replace(" ", "").split("of")[1])
    
    # GET SIG STRIKES *CLINCH* + ATTEMPTS
    clinch_strikes = columns[7].select('p')
    fighter1_sig_str['sig str clinch'] = int(clinch_strikes[0].get_text().strip().replace(" ", "").split("of")[0])
    fighter1_sig_str['sig str clinch att'] = int(clinch_strikes[0].get_text().strip().replace(" ", "").split("of")[1])
    
    fighter2_sig_str['sig str clinch'] = int(clinch_strikes[1].get_text().strip().replace(" ", "").split("of")[0])
    fighter2_sig_str['sig str clinch att'] = int(clinch_strikes[1].get_text().strip().replace(" ", "").split("of")[1])
    
    # GET SIG STRIKES *GROUND* + ATTEMPTS
    ground_strikes = columns[8].select('p')
    fighter1_sig_str['sig str ground'] = int(ground_strikes[0].get_text().strip().replace(" ", "").split("of")[0])
    fighter1_sig_str['sig str ground att'] = int(ground_strikes[0].get_text().strip().replace(" ", "").split("of")[1])
    
    fighter2_sig_str['sig str ground'] = int(ground_strikes[1].get_text().strip().replace(" ", "").split("of")[0])
    fighter2_sig_str['sig str ground att'] = int(ground_strikes[1].get_text().strip().replace(" ", "").split("of")[1])
    
    # ~~~~~~~~~~~~~~~~~~~~~~~~~
    # APPEND BOTH FIGHTER STATS TO LIST
    sig_strikes_both_fighters.append(fighter1_sig_str)
    sig_strikes_both_fighters.append(fighter2_sig_str)
    
    return sig_strikes_both_fighters

  # PUTTING FIGHT DETAILS TOGETHER ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  def get_TOTALS_and_SIG_STR_all_rounds_as_soup(self):
    # get TOTALS by ROUNDS. Get all table rows. Delete first useless row
    totals_by_rounds = self.soup.find('table', class_='b-fight-details__table js-fight-table').find_all('tr', class_='b-fight-details__table-row')
    del totals_by_rounds[0]
    
    # get totals for fight. Insert into 1st element (0 index) of rounds list
    totals_for_fight = self.soup.find('tbody', class_='b-fight-details__table-body').find('tr')
    totals_by_rounds.insert(0, totals_for_fight)
    
    # ~~~~~~~~~~~~~~~~~
    
    # get SIG STRIKES by ROUNDS. Get all table rows. Delete first useless row
    sig_str_by_rounds = self.soup.find_all('table', class_='b-fight-details__table js-fight-table')[1].find_all('tr', class_='b-fight-details__table-row')
    del sig_str_by_rounds[0]

    # get sig strikes for fight. Insert into 1st element (0 index) of rounds list
    sig_str_for_fight = self.soup.find_all('tbody', class_='b-fight-details__table-body')[2].find('tr')
    sig_str_by_rounds.insert(0, sig_str_for_fight)
    
    # return item as 2 items
    return [totals_by_rounds, sig_str_by_rounds]
    
  def all_fight_details(self):
    # get totals and sig str
    rounds_clean_soup = self.get_TOTALS_and_SIG_STR_all_rounds_as_soup()
    totals_by_rounds = rounds_clean_soup[0]  
    sig_str_by_rounds = rounds_clean_soup[1]
    
    # Create 2 lists. Each list contains a list of dict, corresponds to stats of a round [{rd0}, {rd1}, {rd2}]
    fighter_1_stats_all_rounds = []
    fighter_2_stats_all_rounds = []
    
    # For each round...
    for round in range(len(totals_by_rounds)):
      # create a new dict for each fighter. init the round. (round 0 is fight overall)
      fighter_1_rd_dict = dict()
      fighter_2_rd_dict = dict()
      fighter_1_rd_dict['round'] = round
      fighter_2_rd_dict['round'] = round
      
       # get totals stats for both fighters. Merge dictionaries for both fighters
      totals_both_fighters = self.get_totals(totals_by_rounds[round])
      fighter_1_rd_dict.update(totals_both_fighters[0])
      fighter_2_rd_dict.update(totals_both_fighters[1])
      
      # get sig strike stats for both fighters. Merge dictionaries for both fighters
      sig_str_both_fighters = self.get_sig_strikes(sig_str_by_rounds[round])
      fighter_1_rd_dict.update(sig_str_both_fighters[0])
      fighter_2_rd_dict.update(sig_str_both_fighters[1])
      
      # append round details to main list for both fighters
      fighter_1_stats_all_rounds.append(fighter_1_rd_dict)
      fighter_2_stats_all_rounds.append(fighter_2_rd_dict)

    # after ever round done, add both fighters into a list and return both
    return [fighter_1_stats_all_rounds, fighter_2_stats_all_rounds]

  
  # PUTTING BOUT TOGETHER !!! ~~~~~~~~~~~~~~~~~~~
  def full_bout_details(self):
    # init a dict
    bout_details = dict()
    some_bout_details = self.get_some_bout_details()
    both_fighter_fight_details = self.all_fight_details()
    
    # ~~~
    bout_details['fighter 1 name'] = self.get_fighter_1_name()
    bout_details['fighter 2 name'] = self.get_fighter_2_name()
    bout_details['fighter 1 result'] = self.get_fighter_1_result()
    bout_details['fighter 2 result'] = self.get_fighter_2_result()
    bout_details['weightclass'] = self.get_weightclass()
    bout_details['win method'] = some_bout_details['win method']
    bout_details['referee'] = some_bout_details['referee']
    bout_details['scheduled rounds'] = some_bout_details['scheduled rounds']
    bout_details['ending round'] = some_bout_details['ending round']
    bout_details['ending time min'] = some_bout_details['ending time min']
    bout_details['ending time sec'] = some_bout_details['ending time sec']
    bout_details['finish details'] = some_bout_details['finish details']
    
    # if finish details is 'scorecards', add a kvp with values being a dict containing scorecards
    if (bout_details['finish details'] == 'scorecards'):
      bout_details['scorecards'] = some_bout_details['scorecards']
      
    bout_details['fighter 1 stats'] = both_fighter_fight_details[0]
    bout_details['fighter 2 stats'] = both_fighter_fight_details[1]
    
    # ~~~
    return bout_details
  
  # TEST PRINTS ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

  def test_print_get_some_bout_details(self):
    bout_details = self.get_some_bout_details()
    
    for key, value in bout_details.items():
      print(f"{key} : {value}")

  def test_print_all_fight_details(self, fighter_data = None):
    if (fighter_data == None):
      fighter_data = self.all_fight_details()
    
    else:
      for fighter in fighter_data:
        print("\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~" * 3)
        for rounds in fighter:
          print(f"\nround: {rounds['round']}~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n")
          for stat, info in rounds.items():
            print(f"{stat}: {info}")


  def test_print_full_bout_details(self):
    data = self.full_bout_details()
    fighter_data = []
    
    # for all items in the dictionary
    for key, value in data.items():
       if (key == 'scorecards'):
         for k, v in value.items():
           print(f"{k} : {v}")

       elif ('fighter 1 stats' in key):
          if(len(fighter_data) == 0):
            fighter_data.append(value)
          else:
            fighter_data.insert(0, value)
        
       elif ('fighter 2 stats' in key):
            fighter_data.append(value)
    
       else:
          print(f"{key} : {value}")
          

    self.test_print_all_fight_details(fighter_data)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def main():
  
  # read html file as soup
  #with open('html_templates_to_test/KarolRosa_v_LinaLansberg.html', 'r') as fhandle:
   # soup = BeautifulSoup(fhandle, 'lxml')
    
    #soup = scraper_functions.request("http://ufcstats.com/event-details/a23e63184c65f5b8")
    
    
    scraper = Get_fight_details(soup)
    print(scraper.full_bout_details())
    #scraper.test_print_full_bout_details()

    
if __name__ == '__main__':
  main()
  
