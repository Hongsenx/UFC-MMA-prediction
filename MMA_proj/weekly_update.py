from time import sleep
import traceback
import sys
sys.path.append("scraper_files")
import scraper_functions
from scraper_files.scrape_basic_events import basic_events_scraper
from scraper_files.scrape_athletes import athletes_scraper
from scraper_files.scrape_events import events_scraper
from scraper_files.update_db import db_updater

# logging ~~~~
import logging
def set_logger():
  logging.basicConfig(
    level=logging.INFO,
    format="{asctime} {levelname:<8} {message}",
    style = "{",
    filemode="a+")

  # create logger
  logger = logging.getLogger()
  handler = logging.FileHandler('scraper_text_logs.log')
  formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
  handler.setFormatter(formatter)
  logger.addHandler(handler)
  
  return logger
logger = set_logger()

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# DEFINING WEEKLY UPDATER CLASS
class weekly_updater():
  
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # OBJECT INITIALISER WITH TIME DELAY
  def __init__(self, time_delay=[0,2]):
    self.time_delay = time_delay

  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # WEEKLY UPDATE
  def weekly_update(self):
    error_counter = 0

    logger.info("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
    logger.info("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
    logger.info("~~~ UPDATING PAST EVENTS BASIC DATA ~~~")
    # update past events
    try:
      # init basic events scraper and call function to update
      basic_events_updater = basic_events_scraper(self.time_delay)
      basic_events_updater.update_past_events_basic_JSON()
    except:
      traceback.print_exc()
      logger.critical("problem updating past events' basic data to json")
      logger.critical("Could not proceed with other updates")
      error_counter += 1
    else:
      # update events ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
      sleep(2)
      logger.info("~~~ UPDATING EVENTS DATA ~~~")
      # init events scraper and call function to update
      event_scraper = events_scraper(self.time_delay)
      if(event_scraper.update_events_details()) == False:
        logger.error('failed to update events')
        error_counter += 1
        
      # update athlete profiles ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
      sleep(2)
      logger.info("~~~ UPDATING ATHLETE PROFILES DATA ~~~")
      # init athlete scraper and call function to update
      athlete_scraper = athletes_scraper(self.time_delay)
      if(athlete_scraper.update_fighter_details()) == False:
        logger.error('failed to athlete data')
        error_counter += 1
        
      # update database only there are no issues with the previous functions
      logger.info("~~~ UPDATING DATABASE ~~~")
      if (error_counter == 0):
        # update database ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        sleep(2)
        # init database updater and call function to update
        database_updater = db_updater('ufc_db.db')
        if (database_updater.update_db() == False):
          logger.error('failed to update database')
          error_counter += 1
      else:
        logger.info('database not updated')

    # print complete message + error status (if any) ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    logger.info("~~~ STATUS OF WEEKLY UPDATE ~~~")
    if (error_counter != 0):
      logger.info(f"Weekly update NOT completed. Num of errors: {error_counter}")
    else:  
      logger.info(f"Weekly update completed. No errors.")

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def main():
  scraper = weekly_updater([0,2])
  scraper.weekly_update()
 
if __name__ == "__main__":
  main()