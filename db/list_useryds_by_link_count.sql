SELECT
	useryd.useryd_id, useryd.user_reddit_name, COUNT(*)
FROM
	useryd
LEFT JOIN
	activesubreddit_yd ON useryd.useryd_id = activesubreddit_yd.useryd_id
GROUP BY
	activesubreddit_yd.useryd_id, useryd.useryd_id, useryd.user_reddit_name
ORDER BY
	COUNT(*) ASC
LIMIT 100;