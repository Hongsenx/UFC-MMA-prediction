
import sys
sys.path.append("../")
import scraper_functions

duplicate = []
profiles = scraper_functions.read_json_data('scraped_data/fighter_profile.json')

# for each fighter name
for profile in profiles:
  # get name
  name_to_check = profile['name']
  counter = 0
  
  for profile in profiles:
    if (profile['name'] == name_to_check):
      counter += 1
  
  if (counter != 1):
    duplicate.append(name_to_check)

print(duplicate) 