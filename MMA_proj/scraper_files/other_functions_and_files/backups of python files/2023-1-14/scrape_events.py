from datetime import datetime
from bs4 import BeautifulSoup
import traceback
import os
import scraper_functions
# logging ~~~~
import logging
logger = logging.getLogger(__name__)

from get_fight_details import Get_fight_details

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Defining scraper class
class events_scraper():
  
  # INIT AN OBJECT INSTANCE WITH TIME DELAY
  def __init__(self, time_delay = [0,3]):
    self.time_delay = time_delay

  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # REQUEST FUNCTION FOR THIS SCRAPER
  def make_request(self, url):
    return (scraper_functions.request(url, False, self.time_delay))
  
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # function 1/2 to get full event details
  def get_fight_links_from_event(self, event_soup):
    # intput: a url to an event
    # returns: a list of fight links
    
    # make a list of fight links
    fight_links = []

    # get table then get list of
    list_of_table_rows = event_soup.find('tbody', class_='b-fight-details__table-body').select('tr')
    
    # loop through row to get links to fight
    for row in list_of_table_rows:
      fight_links.append(row['data-link'])
      
    return fight_links

  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # function 2/2 to get full event details (uses another script)
  def get_data_from_a_fight(self, fight_link):
  # input: a soup obj for 1 fight
  # returns:
  #   1) whether scrape was a success
  #   2) a dictionary (if incomplete, dictionary will be with miminal info)
    
    # init a dictionary. init a boolean to indicate whether scrape was successful
    fight_data = dict()
    scrape_success = True
    
    # make a request to get soup for fight
    soup_fight = self.make_request(fight_link)
    
    # init a scraper_obj and pass soup obj into function (separate py script)
    fight_scraper_obj = Get_fight_details(soup_fight)
    
    # call function to get fight details. Wrap in a try, except block
    try:
      fight_data = fight_scraper_obj.full_bout_details()
      
    # if error, init the dictionary 
    except:
      fight_data['fight url'] = fight_link
      scrape_success = False
      
    return [scrape_success, fight_data]
    
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # function to get full event details
  def get_full_event_data(self, basic_event_info):
    # input: a dictionary from 'past_events' json file
    # output: a dictionary object with event info and all fight data from that event
    
    # init a dictionary for the fight event
    event_dict = dict()
    event_dict['event name'] = basic_event_info['event name']
    event_dict['date'] = basic_event_info['date']
    event_dict['country'] = basic_event_info['country']
    event_dict['state'] = basic_event_info['state']
    event_dict['data completeness'] = 0
    event_dict['fights'] = []
    
    # make a request and get event soup
    event_soup = self.make_request(basic_event_info['event url'])
    
    # get all fight links for that event
    event_fight_links = self.get_fight_links_from_event(event_soup)
    
    # init a var to get data completeness
    total_num_fights = len(event_fight_links)
    num_scrapped_successful = 0
    
    # for each fight...
    for i, fight_link in enumerate(event_fight_links):
      
      # init a dict for fight data
      fight_data = {"fight number": i + 1}
      
      # call function to get scrapped fight data. Merge 2nd dictionary with the first dictionary
      scrapped_data = self.get_data_from_a_fight(fight_link)
      fight_data.update(scrapped_data[1])
      
      # if scrape successful, increment counter
      if (scrapped_data[0] == True):
        num_scrapped_successful += 1
    
      # append to dictionary
      event_dict["fights"].append(fight_data)
      
    # after looping through to get all fights for the event, calc data completeness
    event_dict["data completeness"] = round((num_scrapped_successful/total_num_fights)*100)

    return event_dict
    
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # Write event data to JSON
  def write_1_event_data_to_json(self, event_dict):
    # input: a dict corresponding to a fight event
    # return: nil
    
    # init var for info to be written
    info_to_write = []
    
    # get the names of all files for each year of fights
    # first, get current working directory, concat the directory path
    folder_path = os.getcwd() + r"\scraper_files\scraped_data\all_fights_by_year"
    file_names= os.listdir(folder_path)
    
    # get the date and name of the event
    this_event_name = event_dict['event name']
    this_event_date = event_dict['date']
    this_event_datetime_obj = datetime.strptime(event_dict['date'], '%d/%b/%Y')
    this_event_year = event_dict['date'].split('/')[2]
    
    # loop through file names
    for file in file_names:
      # if there is file for the year of this event...
      if (this_event_year in file):
        # Get file path. Then read file contents. Also, create a duplicate 
        file_path = r'scraper_files\scraped_data\all_fights_by_year'
        file_path += f'\{this_event_year}_events.json'
        file_data = scraper_functions.read_json_data(file_path)
        file_data_duplicate = file_data.copy()
        
        event_written_before = False
        # Loop through dicts in file. One dict = 1 event.
        
        # If event HAS been written before, we have to replace it ~~~
        for i, event in enumerate(file_data):
          # If there is a dict with same date and name
          if ((this_event_date == event['date']) and (this_event_name == event['event name'])):
            # replace the old event (in duplicate list) with this event dict
            file_data_duplicate[i] = event_dict
            event_written_before = True
            break
            
        # If event HAS NOT been written before, we insert according to date
        if (event_written_before == False):
          for i, event in enumerate(file_data):
            # get date, convert to datetime obj
            event_datetime = datetime.strptime(event['date'], '%d/%b/%Y')

            # if curr event is more recent than the compared, insert (into duplicate list) at this index
            if (this_event_datetime_obj > event_datetime):
              file_data_duplicate.insert(i, event_dict)
              event_written_before = True
              break
            
          # If event is earlier than all other events...
          if (event_written_before == False):
            file_data_duplicate.append(event_dict)
          
        # write the event to the current file. Update log + return
        scraper_functions.save_list_as_json(list=file_data_duplicate, indent_settings = 1, file_name=file_path)
        logger.info(f'Event saved in json: {this_event_name}')
        return
        
    # If no file for this event's year...Make a new file.
    new_file_path_name = r'scraper_files\scraped_data\all_fights_by_year'
    new_file_path_name += f'\{this_event_year}_events.json'
    info_to_write = [event_dict]
    
    # write the event to the NEW file and return
    scraper_functions.save_list_as_json(list=info_to_write, indent_settings = 1, file_name=new_file_path_name)
    
    # ADD LOG + RETURN
    logger.info(f'Event saved in json: {this_event_name}') 
    return
  
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  def get_events_to_be_updated(self):
    list_of_events_to_be_updated = []
    
    # get the date of last update 
    last_update = scraper_functions.check_field_scraper_date_logs('update past events to JSON')
    last_update = datetime.strptime(last_update, '%d/%b/%Y %H:%M:%S')
    
    # read past events basic info from json
    file_path_past_events = 'scraper_files/scraped_data/past_events.json'
    past_events_basic_info_dicts = scraper_functions.read_json_data(file_path_past_events)
    
    # loop through past events (basic data)
    for event in past_events_basic_info_dicts: 
      # if the date of the event is after / same as last updated date...
      if (datetime.strptime(event['date'], '%d/%b/%Y') >= last_update):
        list_of_events_to_be_updated.append(event)
      # if date of event is before last updated date, break loop 
      else:
        break
    
    return list_of_events_to_be_updated
  
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  def update_events_details(self, specific_event_basic_info_dict = None):
    # return True if successful. return False if unsuccessful
    
    # init list for events to be updated
    list_events_to_update = []
    
    # if a specific event to be updated...
    if (specific_event_basic_info_dict):
      list_events_to_update.append(specific_event_basic_info_dict)
    else:
      # Get date of the last scrapped item and return a list of events (of basic event dicts) to be updated
      list_events_to_update = self.get_events_to_be_updated()
      
      # if no events to update, update text log and return
      if len(list_events_to_update) == 0:
        logger.info('No events to update (events)')
        return True

      
    # init a list for events to count update
    num_updated = 0
    
    # loop through all events to update and update it...duh
    for event in list_events_to_update:
      try:
        full_event_data_dict = self.get_full_event_data(event)
      except:
        traceback.print_exc()
        logger.error(f"Problem with function 'get_full_event_data(self, event)' for: {event['event name']}")
        
      else:
        try:
          self.write_1_event_data_to_json(full_event_data_dict)
          num_updated += 1
        except:
          traceback.print_exc()
          logger.error(f"Problem with function 'write_1_event_data_to_json(full_event_data_dict)' for: {event['event name']}")
          return False

    # if not updating specific event
    if (specific_event_basic_info_dict == None):
      # if num_updated at least 1, add to logging + update date on scraper logs
      if (num_updated > 0):
        logger.info(f'number of events updated: {num_updated}')
        scraper_functions.update_scraper_date_logs('update past events to JSON')
        return True
  
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def main():
  scraper = events_scraper([0,3])
  
  #scraper.update_events_details()
  #scraper.update_events_details({"event name": "UFC 275: Teixeira vs. Prochazka", "date": "11/Jun/2022", "event url": "http://ufcstats.com/event-details/3a24769a4855040a", "country": "Singapore", "state": "Kallang"})

if __name__ == "__main__":
  main()
