SELECT 
	(SELECT COUNT(*) FROM reddit_data.useryw where week=1) 
		as 'week 1 users',
    (SELECT COUNT(*) FROM reddit_data.useryw where week=2) 
		as 'week 2 users',
	(SELECT COUNT(*) FROM reddit_data.useryw where week=3) 
		as 'week 3 users',
    (SELECT COUNT(*) FROM reddit_data.useryw where week=4) 
		as 'week 4 users',	
	(SELECT COUNT(*) FROM reddit_data.useryw where week=5) 
		as 'week 5 users',
    (SELECT COUNT(*) FROM reddit_data.useryw where week=6) 
		as 'week 6 users',
	(SELECT COUNT(*) FROM reddit_data.useryw where week=7) 
		as 'week 7 users',
    (SELECT COUNT(*) FROM reddit_data.useryw where week=8) 
		as 'week 8 users',
    (SELECT COUNT(*) FROM reddit_data.useryw where week=9) 
		as 'week 9  users';