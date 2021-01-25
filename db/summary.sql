SELECT 
	(SELECT COUNT(*) FROM reddit_data.user) 
		as 'users',
	(SELECT COUNT(*) FROM reddit_data.active_subreddits) 
		as 'links',
	(SELECT COUNT(*) FROM reddit_data.subreddit) 
		as 'subreddits',
    (SELECT (SELECT COUNT(*) FROM reddit_data.active_subreddits)/(SELECT COUNT(*) FROM reddit_data.user)) 
		as 'avg links per user';