SELECT
	(SELECT COUNT(*) FROM useryw)
		as 'weekly users',
	(SELECT COUNT(DISTINCT(user_reddit_id)) FROM useryw) 
		as 'unique users',
	(SELECT COUNT(*) FROM activesubreddit) 
		as 'links',
	(SELECT COUNT(*) FROM subreddit)
		as 'subs',
    ((SELECT COUNT(*) FROM activesubreddit)
     /(SELECT COUNT(*) FROM useryw)) 
		as 'link/user',
	(SELECT MAX(linkcount) FROM (
		SELECT COUNT(asub.useryw_id) linkcount
		FROM useryw AS uyw 
		JOIN activesubreddit AS asub ON uyw.useryw_id = asub.useryw_id 
		GROUP BY asub.useryw_id ) AS T)
		as 'max link',
	(SELECT AVG(linkcount) FROM (
		SELECT COUNT(asub.useryw_id) linkcount
		FROM useryw AS uyw 
		JOIN activesubreddit AS asub ON uyw.useryw_id = asub.useryw_id 
		GROUP BY asub.useryw_id ) AS T)
		AS 'ave link';