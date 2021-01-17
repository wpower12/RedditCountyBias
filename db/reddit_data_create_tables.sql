CREATE TABLE `user` (
  `user_id` varchar(45) NOT NULL,
  `user_name` varchar(45) DEFAULT NULL,
  `home_subreddit` varchar(45) DEFAULT NULL,
  PRIMARY KEY (`user_id`),
  UNIQUE KEY `user_id_UNIQUE` (`user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
CREATE TABLE `subreddit` (
  `subreddit_id` varchar(45) NOT NULL,
  `subreddit_name` varchar(64) DEFAULT NULL,
  `subreddit_url` varchar(64) DEFAULT NULL,
  PRIMARY KEY (`subreddit_id`),
  UNIQUE KEY `subreddit_id_UNIQUE` (`subreddit_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
CREATE TABLE `active_subreddits` (
  `user_id` varchar(45) NOT NULL,
  `subreddit_id` varchar(45) NOT NULL,
  `year` int NOT NULL,
  `unique_hash` varchar(90) NOT NULL,
  PRIMARY KEY (`unique_hash`),
  UNIQUE KEY `unique_hash_UNIQUE` (`unique_hash`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
