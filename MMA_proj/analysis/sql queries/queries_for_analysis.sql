
/* FIGHT ANALYSIS */
--select all fights by weight
--count strike differential & select a fight by strike differential
-- metric for one-sidedness of the fight

--select fights by year   (how to sort by date?)
SELECT fights.id, fighter_1_name, fighter_2_name, ending_rd, win_method
FROM fights JOIN events ON events.id = fights.id
WHERE date like '%2022';

--select all fights that ended by (win method) insert parameter
SELECT fights.id
FROM fights
WHERE win_method = '' -- insert parameter (maybe we should mix more of the different decisions)

/* FIGHTER ANALYSIS analysis  fights analysis */ 

--select last (num of fights) of a fighter (sorted)
--select fighter by consistency

--Select athlete by age   (can we select by age and then subtract the current year?)
--classify by fight style

-- get id of all fights of a fighter

SELECT fight_id
FROM fight_stats 
WHERE fighter_id = ?
AND round = 0;

SELECT events.date, fights.id
FROM events
JOIN fights ON events.id = fights.event_id
WHERE fights.id = 
    (SELECT fight_id
    FROM fight_stats
    WHERE fighter_id = ?
    AND round = 0);


-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
--select fighter by number of fights more than __

SELECT id
FROM fighters
WHERE num_fights > 3
    (SELECT COUNT (*)
    FROM fight_stats
    WHERE fighter_id = id
    AND round = 0) AS num_fights

