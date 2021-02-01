SELECT sname.subreddit_name, COUNT(*) as activeusers
FROM reddit_data.activesubreddit as a
    JOIN reddit_data.useryw    as u on a.useryw_id=u.useryw_id
    JOIN reddit_data.subreddit as s on u.home_subreddit=s.subreddit_id
    JOIN reddit_data.county    as c on s.county_id=c.county_id
    JOIN reddit_data.subreddit as sname on a.subreddit_id = sname.subreddit_id
WHERE 
	c.county_id=129 AND 
    u.week = 1      AND
    sname.subreddit_id <> u.home_subreddit
GROUP BY sname.subreddit_name
ORDER BY activeusers DESC;