import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine, func
from load import RedditPost, Base

def create_dashboard():
    st.set_page_config(page_title="Reddit Sentiment Dashboard", layout="wide")
    st.title("📊 Reddit Sentiment Analysis Dashboard")
    
    # Database connection
    engine = create_engine('sqlite:///data/reddit_posts.db')
    
    # Load data
    query = "SELECT * FROM reddit_posts"
    df = pd.read_sql(query, engine)
    
    if df.empty:
        st.warning("No data available. Run the ETL pipeline first.")
        return
    
    # Sidebar filters
    st.sidebar.header("Filters")
    selected_subreddit = st.sidebar.selectbox("Subreddit", df['subreddit'].unique())
    selected_keyword = st.sidebar.selectbox("Keyword", df['keyword_searched'].unique())
    
    # Filter data
    filtered_df = df[
        (df['subreddit'] == selected_subreddit) & 
        (df['keyword_searched'] == selected_keyword)
    ]
    
    # Metrics
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Posts", len(filtered_df))
    with col2:
        avg_sentiment = filtered_df['title_sentiment'].mean()
        st.metric("Avg Sentiment", f"{avg_sentiment:.2f}")
    with col3:
        positive_pct = (filtered_df['sentiment_category'] == 'positive').mean() * 100
        st.metric("Positive Posts", f"{positive_pct:.1f}%")
    
    # Sentiment distribution chart
    fig1 = px.pie(
        filtered_df, 
        names='sentiment_category', 
        title='Sentiment Distribution'
    )
    st.plotly_chart(fig1)
    
    # Sentiment over time
    fig2 = px.line(
        filtered_df.sort_values('created_utc'),
        x='created_utc',
        y='title_sentiment',
        title='Sentiment Over Time'
    )
    st.plotly_chart(fig2)
    
    # Raw data table
    st.subheader("Recent Posts")
    st.dataframe(
        filtered_df[['title', 'author', 'score', 'sentiment_category', 'created_utc']]
        .sort_values('created_utc', ascending=False)
        .head(10)
    )

if __name__ == "__main__":
    create_dashboard()