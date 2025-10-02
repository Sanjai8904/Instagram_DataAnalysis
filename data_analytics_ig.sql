-- social_media.sql
-- Schema for MySQL Workbench

CREATE TABLE IF NOT EXISTS posts (
  post_id VARCHAR(50) PRIMARY KEY,
  date DATETIME,
  content_type VARCHAR(50),
  caption TEXT,
  likes INT,
  comments INT,
  shares INT,
  views INT,
  hashtags TEXT,
  followers_count INT,
  day_of_week VARCHAR(20),
  hour INT,
  caption_word_count INT,
  engagement INT,
  engagement_rate DECIMAL(10,4)
);

-- 1) Top 10 posts by engagement
SELECT post_id, date, content_type, likes, comments, shares,
       (likes + comments + shares) AS engagement
FROM posts
ORDER BY engagement DESC
LIMIT 10;

-- 2) Average engagement by day of week
SELECT day_of_week, AVG(engagement) AS avg_engagement
FROM posts
GROUP BY day_of_week
ORDER BY avg_engagement DESC;

-- 3) Average engagement by hour
SELECT hour, AVG(engagement) AS avg_engagement
FROM posts
GROUP BY hour
ORDER BY hour;

-- 4) Average engagement by content type
SELECT content_type, AVG(engagement) AS avg_engagement
FROM posts
GROUP BY content_type
ORDER BY avg_engagement DESC;

-- 5) Posts with engagement_rate > 0.05 (5%)
SELECT post_id, engagement_rate, followers_count
FROM posts
WHERE engagement_rate > 0.05
ORDER BY engagement_rate DESC;

-- 6) Top hashtags (⚠️ here hashtags are stored as comma/space separated text)
-- This just counts rows with same hashtags string (not split into individual words)
SELECT hashtags, COUNT(*) AS freq
FROM posts
WHERE hashtags <> ''
GROUP BY hashtags
ORDER BY freq DESC
LIMIT 20;
