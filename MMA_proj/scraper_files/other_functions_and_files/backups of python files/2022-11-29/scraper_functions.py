from random import choice, randint
from time import sleep
import requests
from bs4 import BeautifulSoup
from fake_headers import Headers
from datetime import datetime
import json
import csv
import logging
logger = logging.getLogger(__name__)
# for accessing 'scraper_date_logs.json'


# get a random user agent ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def get_random_user_agent():
  # all agent files
  agent_files =  ["user_agents\Android+Webkit+Browser.txt", "user_agents\Chrome.txt", "user_agents\Edge.txt", "user_agents\Firefox.txt", "user_agents\Opera.txt", "user_agents\Safari.txt"]
  six_agents = []
  
  # get 1 agent from each file and append to list of six agents
  for file in agent_files:
    with open(file, "r") as file_handle:
      six_agents.append( choice( file_handle.readlines() )[:-1] )
  
  # return random agent
  return choice(six_agents)

# print request status & checks ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def print_status_code(status_code):
  if str(status_code)[0] == "2":
    print("request status: successful")
  elif str(status_code)[0] == "3":
      print("request status: redirection")
  elif str(status_code)[0] == "4":
      print("request status: client error")
  elif str(status_code)[0] == "5":
      print("request status: server error")
  else:
    print("request status: unknown")

# Make request (returns soup obj)~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def request(url, print_status=True, time_delay=[0, 2]):
  # REQUESTING
  #(option 1) set user agent 
  #headers = {"User-Agent": get_random_user_agent()}
  
  # (option 2) use fake_headers library to set fake headers
  headers = Headers(headers=True).generate()

  # time delay
  sleep(randint(time_delay[0],time_delay[1]))
  # request and get data 
  response = requests.get(url, headers=headers)
  
  # if print status is true, print status
  if print_status:
    # print headers & status code
    #print(response.request.headers)
    print_status_code(response.status_code)
   
  # make soup obj
  soup = BeautifulSoup(response.content, "lxml")
  # cusom  encoding
 #soup = BeautifulSoup(response.content, 'html.parser', from_encoding="utf-8")
  
  return soup


# Write HTML to new file (can be read as soup obj) ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# file_name must end with "".html"
def save_html(file_name, soup_obj):
  # find if charset exists
  meta_tags_w_charset = soup_obj.select('meta[charset]')
  if len(meta_tags_w_charset) > 0:
    charset = meta_tags_w_charset[0]['charset']
    encoding = None if charset == None else charset
  
  # write to file
  with open(file_name, 'w+', encoding=encoding) as file:
      file.write(str(soup_obj))  #add '.body' to select only the main things
      

# Write list object to JSON file ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def save_list_as_json(list, file_mode = 'w+', indent_settings=None, file_name="sample.json"): 
  with open(file_name, file_mode, encoding="utf-8") as outfile:
    # write list opening bracket at start of file
    outfile.write('[\n')
    
    # get last item index
    last_item_index = len(list) -1
    
    # for each dict in list, write to file
    for i, dict in enumerate(list):
      json.dump(dict, outfile, indent=indent_settings, ensure_ascii=False)
      # if not last item, add ','
      if i != last_item_index:
        outfile.write(',')
        
      # write new line after every dictionary entry
      outfile.write('\n')
      
    # write list closing bracket at end of file
    outfile.write(']')
    
    
# Read JSON file. takes file_path as argument and returns list/dict ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def read_json_data(file_path):
  with open(file_path, 'r', encoding="utf-8") as file_handler:
    data = json.load(file_handler)
    #print(json.dumps(data, indent=4, ensure_ascii=False))
    return data
  
# Write JSON object to CSV file ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def save_json_as_csv(input_path, output_path):
  data = read_json_data(input_path)
  
  # get all header names and store in var called 'headers'
  headers = []
  for key in data[0].keys():
    headers.append(key)

  # open a new CSV file, set parameters and settings
  with open(output_path, 'w+', newline='', encoding='utf8') as csv_file:
    writer = csv.writer(csv_file, delimiter=',')
    
    # write headers
    writer.writerow(headers)
    
    # write every entry, store values in a list, 
    for dict in data:
      row = []
      for key, value in dict.items():
        row.append(value)
      # write list to CSV
      writer.writerow(row)


# check date of a field in scraper_logs (return as datetime obj) ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def check_field_scraper_date_logs(check_field):
  scraper_logs = read_json_data('scraper_date_logs.json')
  if (check_field not in scraper_logs.keys()):
    return "No such field in scraper date log"
  else:
    # return as datetime obj
    return scraper_logs[check_field]

# Update scraper Logs ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# input: a key in the JSON logs file
# optional input:: a custom date input in format '%d/%b/%Y' eg. (11/Feb/2022)
# returns true/false for update
def update_scraper_date_logs(update_field, date_input=None):
  # read json data
  file_path = 'scraper_date_logs.json'
  logs = read_json_data(file_path)
  
  # if update field is invalid
  if (update_field not in logs.keys()):
    logger.error('error updating scraper date logs: key input not valid')
  
  # if update field is valid
  else:
    # if date input given
    if (date_input != None):
      # convert to to datetime obj. Format to dd/mmm/YY H:M:S
      to_write = datetime.strptime(date_input, '%d/%b/%Y').strftime('%d/%b/%Y %H:%M:%S')
    else:
      to_write = datetime.now().strftime('%d/%b/%Y %H:%M:%S')
    
    # update and write to json
    logs[update_field] = to_write
    with open (file_path, 'w+') as outfile:
      json.dump(logs, outfile, indent=2)

    # log in text logs
    logger.info(f'scraper date logs updated: {update_field}') 











