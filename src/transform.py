import pandas as pd
from textblob import TextBlob
from datetime import datetime
from typing import List, Dict  # This was missing!

class DataTransformer:
    @staticmethod
    def calculate_sentiment(text: str) -> float:
        """Calculate sentiment polarity using TextBlob."""
        if pd.isna(text) or text.strip() == '':
            return 0.0
        return TextBlob(text).sentiment.polarity
    
    @staticmethod
    def categorize_sentiment(polarity: float) -> str:
        """Categorize sentiment based on polarity score."""
        if polarity > 0.1:
            return "positive"
        elif polarity < -0.1:
            return "negative"
        else:
            return "neutral"
    
    def transform_data(self, posts_data: List[Dict]) -> pd.DataFrame:
        """Transform raw Reddit data into structured DataFrame."""
        df = pd.DataFrame(posts_data)
        
        if df.empty:
            return df
        
        # Convert timestamp
        df['created_utc'] = pd.to_datetime(df['created_utc'], unit='s')
        df['extracted_at'] = datetime.utcnow()
        
        # Calculate sentiment
        df['title_sentiment'] = df['title'].apply(self.calculate_sentiment)
        df['sentiment_category'] = df['title_sentiment'].apply(self.categorize_sentiment)
        
        # Data quality checks
        df = df.drop_duplicates(subset=['id'])
        df = df[df['title'].notna()]
        
        return df

# Test function to verify the module works correctly
def test_transformer():
    """Test the DataTransformer with sample data."""
    sample_data = [
        {
            'id': 'test1',
            'title': 'I love this amazing product!',
            'author': 'user1',
            'score': 10,
            'num_comments': 5,
            'created_utc': 1609459200,  # Jan 1, 2021
            'url': 'http://example.com',
            'selftext': 'This is the best product ever',
            'subreddit': 'python',
            'keyword_searched': 'data'
        },
        {
            'id': 'test2',
            'title': 'I hate this terrible product!',
            'author': 'user2',
            'score': 2,
            'num_comments': 3,
            'created_utc': 1609545600,  # Jan 2, 2021
            'url': 'http://example.com',
            'selftext': 'This is the worst product ever',
            'subreddit': 'python',
            'keyword_searched': 'data'
        }
    ]
    
    transformer = DataTransformer()
    result = transformer.transform_data(sample_data)
    
    print("Test completed successfully!")
    print(f"Transformed {len(sample_data)} posts")
    print(f"DataFrame shape: {result.shape}")
    print("\nFirst few rows:")
    print(result.head())
    
    return result

if __name__ == "__main__":
    # Run test if this file is executed directly
    test_transformer()