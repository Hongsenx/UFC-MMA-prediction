SELECT ending_rd, ending_time_min, ending_time_sec, sig_str_percent, sig_str, td_percent, td
FROM fights
JOIN fight_stats ON fights.id = fight_stats.fight_id
WHERE fighter_id = 964
AND fight_id = 6666
AND round = 0;


-- get id of all fights of a fighter
SELECT fight_id
FROM fight_stats
WHERE fighter_id = ?
AND round = 0;

-- select all fight id and date of a fighter (by fighter id)
SELECT events.date, fights.id
FROM events
JOIN fights ON events.id = fights.event_id
WHERE fights.id IN 
    (SELECT fight_id
    FROM fight_stats
    WHERE fighter_id = ?
    AND round = 0);


-- select all fight id and date by of a fighter (by fighter id)
SELECT events.date, fights.id FROM events JOIN fights ON events.id = fights.event_id WHERE weightclass = 'UltimateFighter2HeavyweightTournamentTitle';
