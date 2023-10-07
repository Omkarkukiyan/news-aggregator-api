import httpx
import pandas as pd
from fastapi import FastAPI, HTTPException
from typing import Optional

app = FastAPI()

NEWS_API_LIST_URL = "http://newsapi.org/v2/top-headlines?category=general&pageSize={limit}&page=1&apiKey={api_key}"
NEWS_API_SEARCH_URL = "http://newsapi.org/v2/everything?q={query}&pageSize={limit}&page=1&apiKey={api_key}"
API_KEY = "cd544524f61441d1982ecc8b3aaf9ca5"

REDDIT_BASE_URL = "https://www.reddit.com/r/news/top.json"
REDDIT_SECOND_BASE_URL = "https://www.reddit.com/r/news/search.json?q={query}&limit={limit}"


@app.get("/fetch-reddit-data")
async def reddit_api(query: Optional[str] = None, limit: int = 10):
    # Set default parameters
    params = {
        "limit": limit,
    }
    
    if query:
        params["q"] = query  # Add the query parameter if provided
        async with httpx.AsyncClient() as client:
            response = await client.get(REDDIT_SECOND_BASE_URL, params=params)
    else:
        async with httpx.AsyncClient() as client:
            response = await client.get(REDDIT_BASE_URL, params=params)
    

    if response.status_code == 200:
        # Parse the JSON response
        data = response.json()
        raw_articles = data.get("data", {}).get("children", [])  # Extract the children list

        # Create a list of dictionaries containing the articles' data
        reddit_articles = [article["data"] for article in raw_articles]

        # Create a DataFrame from the articles
        reddit_articles_df = pd.DataFrame(reddit_articles)

        # Extract only the "title" and "url" columns
        reddit_articles_df = reddit_articles_df[["title", "url"]]

        # Add a "source" column with the value "reddit"
        reddit_articles_df["source"] = "reddit"

        # Convert the DataFrame to a list of records (dictionaries)
        reddit_articles_data = reddit_articles_df.to_dict(orient="records")

        # Get Total Count
        reddit_article_count = reddit_articles_df.shape[0]
        
        # Return the data as a JSON response
        return {"totalResults": reddit_article_count, "data": reddit_articles_data}
    else:
        # Handle API request errors
        raise HTTPException(status_code=500, detail="Unable to fetch data from the external API")

        
@app.get("/fetch-news-data")
async def news_api(search: Optional[str] = None, limit: int = 10):  
    params = {
        "apiKey": API_KEY,
        "limit": limit
        }  
    if search:
        params['search'] = search
        # Make an HTTP request to the News API
        async with httpx.AsyncClient() as client:
            response = await client.get(NEWS_API_SEARCH_URL, params=params)
    else:
        # Make an HTTP request to the News API
        async with httpx.AsyncClient() as client:
            response = await client.get(NEWS_API_LIST_URL, params=params)

    if response.status_code == 200:
        # Parse the JSON response
        data = response.json()
        news_articles = data.get("articles", [])
        
        # Create a DataFrame from the articles
        news_articles_df = pd.DataFrame(news_articles)
        
        # Extract only the "title" and "url" columns
        news_articles_df = news_articles_df[["title", "url"]]

        # Add a "source" column with the value "reddit"
        news_articles_df["source"] = "newapi"
        
        # Convert the DataFrame to a list of records (dictionaries)
        news_articles_data = news_articles_df.to_dict(orient="records")

        # Get Total Count
        news_article_count = news_articles_df.shape[0]
        
        # Return the data as a JSON response
        return {"totalResults": news_article_count, "data": news_articles_data}
    else:
        # Handle API request errors
        raise HTTPException(status_code=500, detail="Unable to fetch data from the external API")


