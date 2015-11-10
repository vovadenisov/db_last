USE tp_hw_1;
ALTER TABLE post ADD child_posts_count INT DEFAULT 0;
ALTER TABLE followers ADD CONSTRAINT followers_follower_following_unique UNIQUE (follower_id, following_id);
ALTER TABLE subscriptions ADD CONSTRAINT subscriptions_user_thread_unique UNIQUE (user_id, thread_id);
ALTER TABLE post_hierarchy_utils DROP FOREIGN KEY post_hierarchy_utils_forum_id_foreign_key;
ALTER TABLE post_hierarchy_utils CHANGE forum_id thread_id INT NOT NULL;
ALTER TABLE post_hierarchy_utils ADD CONSTRAINT post_hierarchy_utils_thread_id_foreign_key FOREIGN KEY (thread_id) REFERENCES thread(id);
