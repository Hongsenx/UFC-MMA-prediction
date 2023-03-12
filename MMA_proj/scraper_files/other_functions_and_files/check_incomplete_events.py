import os
import sys
sys.path.append("../")
import scraper_functions

# loop through json file for all years
folder_path = os.getcwd() + r"\scraped_data\all_fights_by_year"
file_names= os.listdir(folder_path)
file_path_front = r'scraped_data\all_fights_by_year\\'

incomplete_events = []
counter = 0

# loop through each event
for name in file_names:
  file_path = file_path_front + name
  
  # open the file
  year_data = scraper_functions.read_json_data(file_path)
  
  # loop through all events
  for event in year_data:
    counter += 1
    
    to_append = dict()
    if (event['data completeness'] != 100):
      to_append['event name'] = event["event name"]
      to_append['event date'] = event["date"]
      
      # append to dictionary
      incomplete_events.append(to_append)
     
    # print event name to check if it has been looped 
    #print(event['event name'])
      
# print all after checking
for event in incomplete_events:
  print(event)