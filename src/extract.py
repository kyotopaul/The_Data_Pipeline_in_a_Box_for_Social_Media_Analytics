import praw
from prawcore.exceptions import PrawcoreException
import pandas as pd
from typing import List, Dict
import time

class RedditExtractor:
    def __init__(self):
        self.reddit = praw.Reddit('DEFAULT', config_interpolation="basic")
    
    def fetch_posts(self, subreddit_name: str, limit: int = 100, keyword: str = None) -> List[Dict]:
        """Fetch posts from a subreddit, optionally filtered by keyword."""
        posts_data = []
        
        try:
            subreddit = self.reddit.subreddit(subreddit_name)
            
            # Determine which method to use based on keyword presence
            if keyword:
                posts = subreddit.search(query=keyword, limit=limit, sort='new')
            else:
                posts = subreddit.new(limit=limit)
            
            for post in posts:
                posts_data.append({
                    'id': post.id,
                    'title': post.title,
                    'author': str(post.author),
                    'score': post.score,
                    'num_comments': post.num_comments,
                    'created_utc': post.created_utc,
                    'url': post.url,
                    'selftext': post.selftext,
                    'subreddit': str(post.subreddit),
                    'keyword_searched': keyword
                })
            
            print(f"Successfully fetched {len(posts_data)} posts from r/{subreddit_name}")
            
        except PrawcoreException as e:
            print(f"Error fetching data from Reddit: {e}")
        
        return posts_data