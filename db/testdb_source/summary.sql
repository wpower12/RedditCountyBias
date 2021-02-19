SELECT
	(SELECT COUNT(*) FROM useryw) 
		as 'weekly users',
	(SELECT COUNT(DISTINCT(user_reddit_id)) FROM useryw) 
		as 'unique users',
	(SELECT COUNT(*) FROM activesubreddit) 
		as 'links',
	(SELECT COUNT(*) FROM subreddit)
		as 'subs',
-- 	(SELECT (COUNT(DISTINCT(week))-1) FROM useryw) 
-- 		as 'weeks processed',
    ((SELECT COUNT(*) FROM activesubreddit)
     /(SELECT COUNT(*) FROM useryw)) 
		as 'link/user';