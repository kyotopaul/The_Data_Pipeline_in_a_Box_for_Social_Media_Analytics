# src/load.py
from sqlalchemy import create_engine, Column, String, Integer, Float, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import pandas as pd
import os
from pathlib import Path
from typing import List, Dict
import sqlite3

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
    def __init__(self, db_path: str = None):
        print("🔄 Initializing database...")
        
        # Set default path if none provided
        if db_path is None:
            self.db_path = Path('data') / 'reddit_posts.db'
        else:
            self.db_path = Path(db_path)
        
        print(f"📁 Database path: {self.db_path}")
        
        # Ensure directory exists
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        print(f"📂 Directory ensured: {self.db_path.parent}")
        
        # Use absolute path for SQLite
        absolute_path = self.db_path.absolute()
        print(f"📍 Absolute path: {absolute_path}")
        
        # Create SQLite connection string
        connection_string = f'sqlite:///{absolute_path}'
        print(f"🔗 Connection string: {connection_string}")
        
        try:
            self.engine = create_engine(connection_string)
            
            # Test connection immediately
            with self.engine.connect() as conn:
                print("✅ Database connection test passed")
            
            # Create tables
            Base.metadata.create_all(self.engine)
            print("✅ Tables created successfully")
            
            self.Session = sessionmaker(bind=self.engine)
            print("✅ Database session configured")
            
        except Exception as e:
            print(f"❌ Database initialization failed: {e}")
            raise
    
    def load_data(self, df: pd.DataFrame):
        """Load transformed data into SQLite database."""
        if df.empty:
            print("📭 No data to load.")
            return
        
        print(f"📦 Preparing to load {len(df)} records...")
        
        session = self.Session()
        
        try:
            # Check for duplicates before insertion
            existing_ids = {result[0] for result in session.query(RedditPost.id).all()}
            new_posts = df[~df['id'].isin(existing_ids)]
            
            if new_posts.empty:
                print("✅ No new posts to insert (all already exist).")
                return
            
            print(f"🆕 Found {len(new_posts)} new posts to insert")
            
            # Convert DataFrame to list of ORM objects
            posts_to_insert = [
                RedditPost(**row) for row in new_posts.to_dict('records')
            ]
            
            # Insert in batches for better performance
            batch_size = 50
            for i in range(0, len(posts_to_insert), batch_size):
                batch = posts_to_insert[i:i + batch_size]
                session.bulk_save_objects(batch)
                session.commit()
                print(f"✅ Inserted batch {i//batch_size + 1}")
            
            print(f"🎉 Successfully loaded {len(posts_to_insert)} new posts to database.")
            
        except Exception as e:
            session.rollback()
            print(f"❌ Error loading data to database: {e}")
            print("💡 Rolling back transaction...")
            raise
        finally:
            session.close()