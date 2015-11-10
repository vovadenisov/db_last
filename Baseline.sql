--CREATE DATABASE tp_hw_1;
--CREATE USER 'technopark_hw'@'localhost' IDENTIFIED BY 'bUp67AxJK90';
--GRANT ALL PRIVILEGES ON tp_hw_1.* TO 'technopark_hw'@'localhost';

USE tp_hw_1;
CREATE TABLE user
(
  id INT NOT NULL AUTO_INCREMENT,
  email VARCHAR(300) NOT NULL,
  name VARCHAR(200) NOT NULL,
  username VARCHAR(200) NOT NULL,
  about TINYTEXT NOT NULL, 
  isAnonymous BOOLEAN NOT NULL DEFAULT false,

  PRIMARY KEY(id),
  CONSTRAINT user_email_unique_index UNIQUE(email)  
) ENGINE=INNODB; 


CREATE TABLE forum
(
  id INT NOT NULL AUTO_INCREMENT,
  name VARCHAR(700) NOT NULL,
  short_name VARCHAR(500) NOT NULL,
  user_id INT NOT NULL,
  
  PRIMARY KEY(id),
  CONSTRAINT forum_name_unique_index UNIQUE (name),
  CONSTRAINT forum_short_name_unique_index UNIQUE (short_name),
  CONSTRAINT forum_user_id_foreign_key FOREIGN KEY (user_id) REFERENCES user (id) ON DELETE CASCADE ON UPDATE CASCADE
  
) ENGINE=INNODB; 

CREATE TABLE thread
(
  id INT NOT NULL AUTO_INCREMENT,
  title VARCHAR(700) NOT NULL,
  slug  VARCHAR(700) NOT NULL,
  date DATETIME NOT NULL,
  message TEXT NOT NULL,
  isClosed BOOLEAN NOT NULL DEFAULT false,
  forum_id INT NOT NULL,
  user_id INT NOT NULL,
  dislikes INT NOT NULL DEFAULT 0,
  likes INT NOT NULL DEFAULT 0,
  
  isDeleted BOOLEAN NOT NULL DEFAULT false, 
 
  PRIMARY KEY (id),
  INDEX thread_date_index (date),
  CONSTRAINT thread_user_id_foreign_key FOREIGN KEY (user_id) REFERENCES user(id) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT thread_forum_id_foreign_key FOREIGN KEY(forum_id) REFERENCES forum(id) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=INNODB;


CREATE TABLE post
(
  id INT NOT NULL AUTO_INCREMENT,
  hierarchy_id VARCHAR(200) NOT NULL,
  date DATETIME NOT NULL,
  message TEXT NOT NULL,
  user_id INT NOT NULL,
  forum_id INT NOT NULL,
  thread_id INT NOT NULL,
  dislikes INT NOT NULL DEFAULT 0,
  likes INT NOT NULL DEFAULT 0,

  isApproved BOOLEAN NOT NULL DEFAULT false,
  isHighlighted BOOLEAN NOT NULL DEFAULT false,
  isEdited BOOLEAN NOT NULL DEFAULT false,
  isSpam BOOLEAN NOT NULL DEFAULT false,
  isDeleted BOOLEAN NOT NULL DEFAULT false,
  parent_id INT NULL,

  PRIMARY KEY(id),
  CONSTRAINT post_hierarchy_id_unique_index UNIQUE(hierarchy_id),
  INDEX post_date_index (date),
  CONSTRAINT post_user_id_foreign_key FOREIGN KEY(user_id) REFERENCES user(id) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT post_forum_id_foreign_key FOREIGN KEY(forum_id) REFERENCES forum(id) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT post_thread_id_foreign_key FOREIGN KEY(thread_id) REFERENCES thread(id) ON DELETE CASCADE ON UPDATE CASCADE
  
) ENGINE=INNODB;

CREATE TABLE subscriptions
(
  id INT NOT NULL AUTO_INCREMENT,
  user_id INT NOT NULL,
  thread_id INT NOT NULL,

  PRIMARY KEY(id),
  CONSTRAINT subscriptions_thread_id_foreign_key FOREIGN KEY(thread_id) REFERENCES thread(id) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT subscriptions_user_id_foreign_key FOREIGN KEY(user_id) REFERENCES user(id) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=INNODB;

CREATE TABLE followers
(
  id INT NOT NULL AUTO_INCREMENT,
  follower_id INT NOT NULL,
  following_id INT NOT NULL,

  PRIMARY KEY(id),
  CONSTRAINT followers_thread_id_foreign_key FOREIGN KEY(follower_id) REFERENCES user(id) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT followers_following_id_foreign_key FOREIGN KEY(following_id) REFERENCES user(id) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=INNODB;

--Сквозная нумерация постов в рамках одного thread
CREATE TABLE post_hierarchy_utils
(
  id INT NOT NULL AUTO_INCREMENT,
  forum_id INT NOT NULL,
  head_posts_number INT NOT NULL DEFAULT 0,

  PRIMARY KEY(id),
  CONSTRAINT post_hierarchy_utils_forum_id_foreign_key FOREIGN KEY(forum_id) REFERENCES forum(id) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=INNODB;




