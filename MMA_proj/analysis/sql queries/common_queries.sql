-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
DELETE FROM (table);
-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
SELECT COUNT(*) FROM events WHERE event_name = '' AND date = '' ;
-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
INSERT INTO (table) (column1, column2) VALUES (value1, value2);
-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

UPDATE fighters
SET total_num_fights = new_value_1,
    win = new_value_2,
    loss = new_value_2,
    draw = new_value_2,
    nc = new_value_2
    win_ratio = new_value_2
WHERE
    name = '{name}'
    AND dob = '{dob}';