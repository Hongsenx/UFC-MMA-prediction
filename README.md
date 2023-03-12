# UFC-MMA-prediction
A multi-part project involving data scraping, data engineering, analysis and machine learning.

The goal of this project is to generate accurate predictions of fight outcomes for UFC events.

## Table of contents
* [Project description](#Project-Description)
* [Future implementations](#Future-implementations)
* [Technologies](#Technologies)

## Project Description
### What the application does (and status)
##### 1) Web scraping (completed)
1) Scraping of data from UFC site.
2) Organisation of data to store in JSON files.
    
##### 2) Data engineering (completed)
1) Manipulation of raw data from JSON files.
2) Loading of transformed data into SQLite database.

##### 3) Analysis (In progress)
1) Pulling of data from SQLite database for quick comparison of competing fighters.
2) Analyses of fighter stats compared to weightclass and previous fights.

##### 4) Generating predictions (planned)
1) Feeding data to machine learning algorithms. (Random Forest and XGboost will be tested first).
2) Saving of predictions to JSON file.

## Future implementations
* Automated weekly program run
* Script to write recently passed bouts to existing CSV file
* JSON file & table in SQLite DB to store upcoming fights and predictions.


## Technologies
* Python 3.11
* SQLite
* Pandas
