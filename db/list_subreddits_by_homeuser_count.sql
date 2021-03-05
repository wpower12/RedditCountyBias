SELECT 
	subreddit.subreddit_name, useryd.day, subreddit.scrape_count, COUNT(*)
FROM
    useryd
LEFT JOIN
    subreddit on useryd.home_subreddit = subreddit.subreddit_id
GROUP BY
    useryd.home_subreddit, useryd.day
ORDER BY
    subreddit.scrape_count ASC, COUNT(*) ASC
LIMIT 100;