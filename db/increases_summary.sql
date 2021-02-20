SELECT
	((SELECT COUNT(*) FROM activesubreddit)-2858330)
		as 'delta links',
	((SELECT COUNT(*) FROM subreddit)-80280)
		as 'delta subs',
    ((SELECT COUNT(*) FROM activesubreddit)
     /(SELECT COUNT(*) FROM useryw)) 
		as 'link/user',
	(SELECT (2858330/651455))
		as 'original ratio',
	(((SELECT COUNT(*) FROM activesubreddit)
     /(SELECT COUNT(*) FROM useryw))-(2858330/651455))
		as 'delta ratio';