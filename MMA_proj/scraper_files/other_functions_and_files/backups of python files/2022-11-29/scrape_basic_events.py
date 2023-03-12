from datetime import datetime
from bs4 import BeautifulSoup
import scraper_functions
# logging ~~~~
import logging
logger = logging.getLogger(__name__)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# DEFINING SCRAPER CLASS
class basic_events_scraper():
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # OBJECT INITIALISER WITH TIME DELAY
  def __init__(self, time_delay=[0,2]):
    self.time_delay = time_delay

  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # REQUEST FUNCTION FOR THIS SCRAPER
  def make_request(self, url):
    return (scraper_functions.request(url, False, self.time_delay))
  
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # UPDATE PAST EVENTS BASIC JSON
  def update_past_events_basic_JSON(self):
    # check date of the most recently scraped event
    file_path_past_events = 'scraper_files/scraped_data/past_events.json'
    past_scrapped_events = scraper_functions.read_json_data(file_path_past_events)
    latest_event_date = past_scrapped_events[0]['date']
    
    # Make request and get soup
    all_completed_events_url = 'http://ufcstats.com/statistics/events/completed?page=all'
    soup = self.make_request(all_completed_events_url)
      
    # init a list to store new events to update
    new_scraped_events = []
    
    # get table with all contents
    table_of_events = soup.find('table', class_='b-statistics__table-events').find('tbody')
    
    # Find all fight sections (excludes upcoming fight).
    list_of_events = table_of_events.find_all('tr', class_='b-statistics__table-row')
    # remove first element (cause it's empty)
    del list_of_events[0]
    
    # loop through each event 
    for item in list_of_events:
      # make a dict
      event_dict = dict()
      
      # get event name
      event_dict['event name'] = item.find('a').get_text().strip()
      
      # get event date. Convert from (%Y-%b-%d) to (%Y-%b-%d) format 
      event_date = item.find('span').get_text().strip().replace(',', '')
      event_dict['date'] = datetime.strptime(event_date, '%B %d %Y').strftime('%d/%b/%Y')   
      
      # if the event date is the same, break. Else, continue the getting data of event
      if (latest_event_date == event_dict['date']):
        break
      else:
        # get event url
        event_dict['event url'] = item.find('a')['href']
        
        # get event country + state
        country_and_state = item.find('td', class_='b-statistics__table-col b-statistics__table-col_style_big-top-padding').get_text().strip().split(',')
        if (len(country_and_state) == 2):
          event_dict['country'] = country_and_state[1].strip()
          event_dict['state'] = country_and_state[0].strip()
        if (len(country_and_state) == 3):
          event_dict['country'] = country_and_state[2].strip()
          event_dict['state'] = country_and_state[1].strip()
        
        # append details to list
        new_scraped_events.append(event_dict)
    
    # save a var for number of new events scrapped
    num_new_events_saved = len(new_scraped_events)
    
    # After end of loop, if there is at least one new event, add old list onto new list
    if (len(new_scraped_events) > 0):
      new_scraped_events.extend(past_scrapped_events)
    
      # write new list to json
      scraper_functions.save_list_as_json(new_scraped_events, 'w+', None, file_path_past_events)
     
    # log in text file + date logs
    logger.info(f'Events (basic info) updated. Num of new events saved: {num_new_events_saved}')
    scraper_functions.update_scraper_date_logs('update past events basic info to JSON')

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def main():
  scraper = basic_events_scraper([0,2])
  scraper.update_past_events_basic_JSON()
 

if __name__ == "__main__":
  main()