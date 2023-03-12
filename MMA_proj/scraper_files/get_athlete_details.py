from bs4 import BeautifulSoup
from datetime import datetime

# TAKE_NOTE: This script is a part of the 'scrape_athletes.py' script. The scrape_athletes.py script is the main script.  

# Maybe remove the weightclas part? ~~~~
def get_athlete_details(soup):
  
  # init var for dictionary to return
  fighter_dict = {
    'name': None, 'height': None, 'weight': None, 'reach': None, 'DOB': None,
    'win': None, 'loss': None, 'draw': None, 'NC': None,
    'sig_str_land_permin': None, 'strike_acc': None, 'sig_str_absorb_permin': None, 'sig_str_def': None,
    'td_avg_per15min': None, 'td_acc': None, 'td_def': None, 'sub_avg_per15min': None}    
  
  # get main body ~~~~~~~
  main_body = soup.find('section', 'b-statistics__section_details').find('div', class_='l-page__container')
  
  # get name ~~~~~~~~~~~~
  fighter_dict['name'] = main_body.find('span', class_='b-content__title-highlight').get_text().strip()

  # get record ~~~~~~~~~~
  record = main_body.find('span', class_='b-content__title-record').get_text().strip().replace('Record: ', '')
  
  if 'NC' in record:
    nc = record.split(' (')[1]
    fighter_dict['NC'] = int(nc.replace(' NC)', ''))
    record = record.split(' (')[0]
  else:
    fighter_dict['NC'] = 0

  fighter_dict['win'] = int(record.split('-')[0])
  fighter_dict['loss'] = int(record.split('-')[1])
  fighter_dict['draw'] = int(record.split('-')[2])
    
  # get basic stats ~~~~~
  basic_stats_body = main_body.find('div', 'b-fight-details b-fight-details_margin-top').find('ul').find_all('li')
  
  # height
  height = basic_stats_body[0].get_text().replace('Height:', '').strip()
  if ('--' in height):
    fighter_dict['height'] = None
  else:
    # convert height to inches
    height_foot = int(height.split("' ")[0])
    height_inches = int(height.split("' ")[1].replace('"', ''))
    fighter_dict['height'] = ((height_foot * 12) + height_inches)
  
  # maybe remove weight class and then the function to be 'get last weight class fought at' 
  # weight
  weight = basic_stats_body[1].get_text()
  if ('--' in weight):
    fighter_dict['weight'] = None
  else:
    fighter_dict['weight'] = int(weight.replace('Weight:', '').replace(' lbs.', ''))
    
  # reach
  reach = basic_stats_body[2].get_text()
  if ('--' in reach):
    fighter_dict['reach'] = None
  else:
    fighter_dict['reach'] = int(reach.replace('Reach:', '').replace('"', ''))
  
  # date of birth
  dob = basic_stats_body[4].get_text().replace('DOB:', '').strip()
  if ('--' in dob):
    fighter_dict['reach'] = None
  else:
    fighter_dict['DOB'] = dob.replace(',', '')
  
    # convert date of birth to another format
    new_date_format = datetime.strptime(fighter_dict['DOB'], '%b %d %Y').strftime('%d/%b/%Y')
    fighter_dict['DOB'] = new_date_format
  
  # get fight stats ~~~~~
  fight_stats_body = basic_stats_body = main_body.find('div', 'b-fight-details b-fight-details_margin-top').find('div', class_='b-list__info-box b-list__info-box_style_middle-width js-guide clearfix').find('div', 'b-list__info-box-left clearfix').find_all('li')
  fighter_dict['sig_str_land_permin'] = float(fight_stats_body[0].get_text().replace('SLpM:','').strip())
  
  # maybe recaculate this?! Or leave this to be computed each time? @@@@@@@@@@@@@@
  fighter_dict['strike_acc'] = int(fight_stats_body[1].get_text().replace('Str. Acc.:', '').replace('%', '').strip())
  fighter_dict['sig_str_absorb_permin'] = float(fight_stats_body[2].get_text().replace('SApM:', '').strip())
  fighter_dict['sig_str_def'] = int(fight_stats_body[3].get_text().replace('Str. Def:', '').replace('%', '').strip())
  fighter_dict['td_avg_per15min'] = float(fight_stats_body[5].get_text().replace('TD Avg.:', '').strip())
  fighter_dict['td_acc'] = int(fight_stats_body[6].get_text().replace('TD Acc.:', '').replace('%', '').strip())
  fighter_dict['td_def'] = int(fight_stats_body[7].get_text().replace('TD Def.:', '').replace('%', '').strip())
  fighter_dict['sub_avg_per15min'] = float(fight_stats_body[8].get_text().replace('Sub. Avg.:', '').strip())
  
  # return a dictionary
  return fighter_dict
