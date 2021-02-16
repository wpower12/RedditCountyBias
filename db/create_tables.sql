-- First we create County. No FK deps. 
CREATE TABLE `county` (
  `county_id` int NOT NULL AUTO_INCREMENT,
  `county_name` varchar(45) NOT NULL,
  `state` varchar(45) NOT NULL,
  `fips` varchar(45) DEFAULT NULL,
  PRIMARY KEY (`county_id`),
  UNIQUE KEY `county_id_UNIQUE` (`county_id`)
) ENGINE=InnoDB AUTO_INCREMENT=1054 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Then Subreddits. Needs County for a FK
CREATE TABLE `subreddit` (
  `subreddit_id` varchar(45) NOT NULL,
  `subreddit_name` varchar(64) DEFAULT NULL,
  `subreddit_url` varchar(64) DEFAULT NULL,
  `county_id` int DEFAULT NULL,
  `tp_mean` float DEFAULT NULL,
  `tp_variance` float DEFAULT NULL,
  PRIMARY KEY (`subreddit_id`),
  UNIQUE KEY `subreddit_id_UNIQUE` (`subreddit_id`),
  KEY `fk_subreddit_1_idx` (`county_id`),
  CONSTRAINT `fk_subreddit_1` FOREIGN KEY (`county_id`) REFERENCES `county` (`county_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Useryw has FKs to Subreddit.
CREATE TABLE `useryw` (
  `useryw_id` int NOT NULL AUTO_INCREMENT,
  `user_reddit_id` varchar(45) DEFAULT NULL,
  `user_reddit_name` varchar(45) DEFAULT NULL,
  `year` int DEFAULT NULL,
  `week` int DEFAULT NULL,
  `home_subreddit` varchar(45) DEFAULT NULL,
  PRIMARY KEY (`useryw_id`),
  KEY `homesub_fk_idx` (`home_subreddit`),
  CONSTRAINT `homesub_fk` FOREIGN KEY (`home_subreddit`) REFERENCES `subreddit` (`subreddit_id`)
) ENGINE=InnoDB AUTO_INCREMENT=355522 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Finally, the activesubreddits, which have FKs to subreddit's, and useryw's
CREATE TABLE `activesubreddit` (
  `subreddit_id` varchar(45) DEFAULT NULL,
  `useryw_id` int DEFAULT NULL,
  KEY `usersubpair_sub_idx` (`subreddit_id`),
  KEY `fk_activesubreddit_2_idx` (`useryw_id`),
  CONSTRAINT `fk_activesubreddit_1` FOREIGN KEY (`subreddit_id`) REFERENCES `subreddit` (`subreddit_id`),
  CONSTRAINT `fk_activesubreddit_2` FOREIGN KEY (`useryw_id`) REFERENCES `useryw` (`useryw_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
