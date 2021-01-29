SELECT COUNT(*), sname.subreddit_name
FROM reddit_data.active_subreddits as a
    JOIN reddit_data.user      as u on a.user_id=u.user_id
    JOIN reddit_data.subreddit as s on u.home_subreddit=s.subreddit_id
    JOIN reddit_data.county    as c on s.county_id=c.county_id
    JOIN reddit_data.active_subreddits as a2 on a2.user_id = u.user_id and a2.subreddit_id <> u.home_subreddit
    JOIN reddit_data.subreddit as sname on a2.subreddit_id = sname.subreddit_id
WHERE c.county_id=129
GROUP by sname.subreddit_name;