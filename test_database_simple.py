# test_database_simple.py
from src.load import DatabaseLoader

def test_database():
    print("🧪 Testing database connection...")
    
    try:
        # Test with verbose output
        loader = DatabaseLoader()
        
        # Test basic functionality
        import pandas as pd
        test_df = pd.DataFrame([{
            'id': 'test123',
            'title': 'Test Post',
            'author': 'test_user',
            'score': 10,
            'num_comments': 5,
            'created_utc': pd.Timestamp('2023-01-01'),
            'url': 'http://test.com',
            'selftext': 'Test content',
            'subreddit': 'test',
            'keyword_searched': 'test',
            'title_sentiment': 0.5,
            'sentiment_category': 'positive',
            'extracted_at': pd.Timestamp.now()
        }])
        
        loader.load_data(test_df)
        print("✅ Database test completed successfully!")
        
    except Exception as e:
        print(f"❌ Database test failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_database()