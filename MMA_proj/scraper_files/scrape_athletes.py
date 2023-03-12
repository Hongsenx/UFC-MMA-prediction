from datetime import datetime
from bs4 import BeautifulSoup
import traceback
import scraper_functions
# logging ~~~~
import logging
logger = logging.getLogger(__name__)

from get_athlete_details import get_athlete_details

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Defining scraper class
class athletes_scraper():
  
  # INIT AN OBJECT INSTANCE WITH TIME DELAY
  def __init__(self, time_delay=[0,3]):
    self.time_delay = time_delay
    
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # REQUEST FUNCTION FOR THIS SCRAPER
  def make_request(self, url):
    return (scraper_functions.request(url, False, self.time_delay))

  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  def get_links_to_fighter_profiles(self, event_soup):
    # list of links to each fighter's url for 1 ufc event
    list_of_profile_links = []
    
    all_bouts = event_soup.find('tbody', class_='b-fight-details__table-body').find_all('tr')
    
    for bout in all_bouts:
      links = bout.find_all('td')[1].find_all('a')
      for link in links:
        url = link['href'].strip()
        list_of_profile_links.append(url)
        
    # return list
    return list_of_profile_links

  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  def get_full_fighters_details_from_1_event(self, event_basic_info):
    # init var to return. List of dictionaries. Each dict is the full details of a ufc fighter
    list_of_full_fighter_details = []
    
    # make a request to the url and get soup
    event_soup = self.make_request(event_basic_info['event url'])
    
    # get all figter links for a UFC event
    fighter_profile_links = self.get_links_to_fighter_profiles(event_soup)
    
    # loop through each link and get a dictionary with full fighter's details
    for link in fighter_profile_links:
      # make a reuqest and get fighter soup
      fighter_soup = self.make_request(link)
      
      # pass soup into function to get a dict
      list_of_full_fighter_details.append(get_athlete_details(fighter_soup))
    
    # return list
    return list_of_full_fighter_details

  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  def update_fighters_for_1_event(self, fighters_to_update):
    # input: dict profiles of new fighters to update
    
    # make a list of names & dob of fighters to update
    names_fighters_to_update = []
    for fighter in fighters_to_update:
      names_fighters_to_update.append(fighter['name'])
    
    # open old list of all figher profiles (aka 'fighter_info.json')
    all_fighter_data = scraper_functions.read_json_data('scraper_files/scraped_data/fighter_profile.json')
    
    # init a list to hold indexes to delete
    index_to_delete = []
    
    # loop through (list of fighters to update)
    for to_update in (fighters_to_update):
      # loop through old list
      for i, old_list_athlete in enumerate(all_fighter_data):
        # if name & DOB of fighter (to update) matches that in old list....
        if (to_update['name'] == old_list_athlete['name']) and (to_update['DOB'] == old_list_athlete['DOB']):
          # append the index of the fighter in old list to know which to delete
          index_to_delete.append(i)

    # sort list to delete in descending order and delete (this is so for loop will work)
    for ele in sorted(index_to_delete, reverse = True):
      del all_fighter_data[ele]
      
    # insert dicts of all fighters to update into all fighter data list (reversed so main event fighters will be at the top)
    for athlete in reversed(fighters_to_update):
      all_fighter_data.insert(0, athlete)
      
    # save updated athlete list into json.
    scraper_functions.save_list_as_json(list=all_fighter_data, file_name='scraper_files/scraped_data/fighter_profile.json')
    
    # update logging file
    names_string = ""
    for i, fighter in enumerate(fighters_to_update):
      names_string += f"{i}) {fighter['name']}"
      names_string += " "
    logger.info(f'fighters updated in json: {names_string}')

  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  def update_fighter_details(self):
    # return True if successful. return False if unsuccessful
    
    # init a list for events to get fighters to update
    list_events_to_update = []
    num_updated = 0
    
    # check scraper logs for when fighter details was last updated. Convert to datetime obj
    last_updated = scraper_functions.check_field_scraper_date_logs('update fighters details to JSON')
    last_updated = datetime.strptime(last_updated, '%d/%b/%Y %H:%M:%S')
    
    # read past events' basic info from json
    file_path_past_events = 'scraper_files/scraped_data/past_events.json'
    past_events_basic_info_dicts = scraper_functions.read_json_data(file_path_past_events)
    
    # loop through past events (basic data)
    for event in past_events_basic_info_dicts: 
      # if the date of the event is after / same as last updated date...
      if (datetime.strptime(event['date'], '%d/%b/%Y') >= last_updated):
        list_events_to_update.append(event)
      # if date of event is before last updated date, break loop 
      else:
        break
    
    # if no events to update, update logs and return
    if len(list_events_to_update) == 0:
      logger.info('No events to update (athletes)')
      return True
    
    # reverse list (so it is in chronological order)
    list_events_to_update.reverse()

    # loop through all events to update and update it...duh
    for event in list_events_to_update:
      try:
        list_of_fighter_dicts = self.get_full_fighters_details_from_1_event(event)
      except:
        traceback.print_exc()
        logger.error(f"Problem with function 'get_full_fighters_details_from_1_event(self, event)' for {event['event name']}")
        return False
      else:
        try:
          self.update_fighters_for_1_event(list_of_fighter_dicts)
          num_updated += len(list_of_fighter_dicts)
          # update logging file + scraper logs
          logger.info(f"Updated {num_updated} fighters for event: {event['event name']} into JSON")
          scraper_functions.update_scraper_date_logs('update fighters details to JSON')
          return True
        except:
          traceback.print_exc()
          logger.error(f"Problem with function 'update_fighters_for_1_event(list_of_fighter_dicts)' for {event['event name']}")
          return False

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def main():
  scraper = athletes_scraper([0,2])
  scraper.update_fighter_details()
 
if __name__ == "__main__":
  main()
