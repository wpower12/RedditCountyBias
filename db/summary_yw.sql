SELECT 
	(SELECT COUNT(*) FROM reddit_data.useryw WHERE year=2020) 
		as 'users',
	(SELECT COUNT(*) FROM reddit_data.activesubreddit) 
		as 'links',
    (SELECT (SELECT COUNT(*) FROM reddit_data.activesubreddit)/
			(SELECT COUNT(*) FROM reddit_data.useryw)) 
		as 'avg links per user';