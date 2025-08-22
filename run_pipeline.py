from src.extract import RedditExtractor
from src.transform import DataTransformer
from src.load import DatabaseLoader
import schedule
import time

def run_etl_pipeline():
    """Main ETL pipeline execution function."""
    print("Starting Reddit Sentiment Pipeline...")
    
    # Extract
    extractor = RedditExtractor()
    raw_data = extractor.fetch_posts(
        subreddit_name="python", 
        limit=50, 
        keyword="data engineering"
    )
    
    # Transform
    transformer = DataTransformer()
    transformed_data = transformer.transform_data(raw_data)
    
    # Load
    loader = DatabaseLoader()
    loader.load_data(transformed_data)
    
    print("Pipeline execution completed successfully!")

if __name__ == "__main__":
    # Run immediately
    run_etl_pipeline()
    
    # Schedule to run every hour (optional)
    # schedule.every().hour.do(run_etl_pipeline)
    # while True:
    #     schedule.run_pending()
    #     time.sleep(1)