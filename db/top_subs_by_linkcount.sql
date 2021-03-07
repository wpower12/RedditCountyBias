SELECT sub.subreddit_name, COUNT(*)
FROM
	activesubreddit as asub 
JOIN
	subreddit as sub ON asub.subreddit_id=sub.subreddit_id
GROUP BY 
	subreddit_name
ORDER BY
	COUNT(*) DESC;