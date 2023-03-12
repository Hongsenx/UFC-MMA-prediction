/* ';' necessary for query to be executed in python. Don't remove! */
--FIGHTERS;
CREATE TABLE fighters (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  dob TEXT,
  height INTEGER,
  reach INTEGER,
  last_weight_fought TEXT);

--FIGHTER_STATS;
CREATE TABLE fighter_stats (
  fighter_id INTEGER UNIQUE NOT NULL,
  num_ufc_fights INTEGER,
  total_num_fights INTEGER,
  win_ratio INTEGER,
  win INTEGER,
  loss INTEGER,
  draw INTEGER,
  nc INTEGER,
  total_ufc_fight_time_sec INTEGER,
  avg_ufc_fight_time_sec INTEGER,
  sig_str_acc INTEGER,
  sig_str_land_permin REAL,
  sig_str_land INTEGER,
  sig_str_att INTEGER,
  kd INTEGER,
  sig_str_abs_permin REAL,
  sig_str_def INTEGER,
  td_acc INTEGER,
  td_land INTEGER,
  td_att INTEGER,
  td_land_avg_per15min REAL,
  sub_att_avg_per15min REAL,
  td_def INTEGER,
  FOREIGN KEY(fighter_id) REFERENCES fighters(id));


--EVENTS;
CREATE TABLE events (
  id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  name TEXT NOT NULL,
  date TEXT NOT NULL,
  country TEXT NOT NULL,
  state TEXT NOT NULL);

--FIGHTS;
CREATE TABLE fights (
  id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  event_id INTEGER NOT NULL,
  fighter_1_name TEXT NOT NULL,
  fighter_2_name TEXT NOT NULL,
  fighter_1_result TEXT NOT NULL,
  fighter_2_result TEXT NOT NULL,
  weightclass TEXT NOT NULL,
  scheduled_rds INTEGER NOT NULL,
  ending_rd INTEGER NOT NULL,
  ending_time_min INTEGER NOT NULL,
  ending_time_sec INTEGER NOT NULL,
  win_method TEXT NOT NULL,
  referee_id INTEGER NOT NULL,
  FOREIGN KEY(event_id) REFERENCES events(id),
  FOREIGN KEY(referee_id) REFERENCES referees(id));

--FIGHT_STATS;
CREATE TABLE fight_stats (
  fighter_id INTEGER NOT NULL,
  fight_id INTEGER NOT NULL,
  round INTEGER NOT NULL,
  kd INTEGER NOT NULL,
  sig_str INTEGER,
  sig_str_att INTEGER,
  sig_str_percent INTEGER,
  total_str INTEGER,
  total_str_att INTEGER,
  total_str_percent INTEGER,
  td INTEGER,
  td_att INTEGER,
  td_percent INTEGER,
  sub_att INTEGER,
  reversal INTEGER,
  ctrl_time_min INTEGER,
  ctrl_time_sec INTEGER,
  sig_str_head INTEGER,
  sig_str_head_att INTEGER,
  sig_str_body INTEGER,
  sig_str_body_att INTEGER,
  sig_str_leg INTEGER,
  sig_str_leg_att INTEGER,
  sig_str_distance INTEGER,
  sig_str_distance_att INTEGER,
  sig_str_clinch INTEGER,
  sig_str_clinch_att INTEGER,
  sig_str_ground INTEGER,
  sig_str_ground_att INTEGER,
  FOREIGN KEY(fighter_id) REFERENCES fighters(id),
  FOREIGN KEY(fight_id) REFERENCES fights(id));


--REFEREES;
CREATE TABLE referees (
  id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  name TEXT NOT NULL);

--JUDGES;
CREATE TABLE judges (
  id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  name TEXT NOT NULL);

--JUDGE SCORE CARDS;
CREATE TABLE judge_score (
  fight_id INTEGER NOT NULL,
  judge_id INTEGER NOT NULL,
  score_card TEXT NOT NULL,
  FOREIGN KEY(fight_id) REFERENCES fights(id),
  FOREIGN KEY(judge_id) REFERENCES judges(id));