SELECT 
	uyw.useryw_id, 
    uyw.user_reddit_name, 
    uyw.scrape_count, 
    COUNT(asub.useryw_id) AS 'links'
FROM
	useryw AS uyw
JOIN
	activesubreddit AS asub ON uyw.useryw_id = asub.useryw_id
WHERE
	uyw.week = 1
GROUP BY 
	useryw_id
ORDER BY
	COUNT(asub.useryw_id) ASC, uyw.scrape_count ASC
LIMIT 50;