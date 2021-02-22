SELECT sr.subreddit_id, sr.subreddit_name
FROM 
	(SELECT DISTINCT(subreddit_id) FROM activesubreddit) AS T
LEFT JOIN -- For all the distinct subreddit_ids (LEFT), get their associated rows in... (RIGHT)
	subreddit as sr ON T.subreddit_id=sr.subreddit_id;