SELECT sname.subreddit_name, COUNT(*) as activeusers
FROM reddit_data.active_subreddits as a
    JOIN reddit_data.user      as u on a.user_id=u.user_id
    JOIN reddit_data.subreddit as s on u.home_subreddit=s.subreddit_id
    JOIN reddit_data.county    as c on s.county_id=c.county_id
    JOIN reddit_data.subreddit as sname on a.subreddit_id = sname.subreddit_id
WHERE c.county_id=129 AND sname.subreddit_id <> u.home_subreddit
GROUP BY sname.subreddit_name
ORDER BY activeusers DESC;