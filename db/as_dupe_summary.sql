SELECT useryw_id, subreddit_id, COUNT(*) FROM activesubreddit
GROUP BY useryw_id, subreddit_id HAVING COUNT(*) > 1
ORDER BY COUNT(*) DESC;