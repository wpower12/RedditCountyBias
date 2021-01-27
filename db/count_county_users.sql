SELECT c.county_name, c.state, COUNT(*) 
FROM reddit_data.user as u
JOIN reddit_data.subreddit as s on u.home_subreddit=s.subreddit_id
JOIN reddit_data.county as c    on s.county_id=c.county_id
WHERE c.county_id=129;