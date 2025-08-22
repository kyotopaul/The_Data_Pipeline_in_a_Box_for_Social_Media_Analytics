from sqlalchemy import create_engine, Column, String, Integer, Float, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import pandas as pd
import os

Base = declarative_base()

class RedditPost(Base):
    __tablename__ = 'reddit_posts'
    
    id = Column(String, primary_key=True)
    title = Column(String)
    author = Column(String)
    score = Column(Integer)
    num_comments = Column(Integer)
    created_utc = Column(DateTime)
    url = Column(String)
    selftext = Column(String)
    subreddit = Column(String)
    keyword_searched = Column(String)
    title_sentiment = Column(Float)
    sentiment_category = Column(String)
    extracted_at = Column(DateTime)

class DatabaseLoader:
    def __init__(self, db_path: str = 'data/reddit_posts.db'):
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self.engine = create_engine(f'sqlite:///{db_path}')
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)
    
    def load_data(self, df: pd.DataFrame):
        """Load transformed data into SQLite database."""
        if df.empty:
            print("No data to load.")
            return
        
        session = self.Session()
        
        try:
            # Check for duplicates before insertion
            existing_ids = {result[0] for result in session.query(RedditPost.id).all()}
            new_posts = df[~df['id'].isin(existing_ids)]
            
            if new_posts.empty:
                print("No new posts to insert.")
                return
            
            # Convert DataFrame to list of ORM objects
            posts_to_insert = [
                RedditPost(**row) for row in new_posts.to_dict('records')
            ]
            
            session.bulk_save_objects(posts_to_insert)
            session.commit()
            print(f"Successfully loaded {len(posts_to_insert)} new posts to database.")
            
        except Exception as e:
            session.rollback()
            print(f"Error loading data to database: {e}")
        finally:
            session.close()