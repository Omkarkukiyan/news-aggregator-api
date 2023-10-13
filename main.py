import httpx, json, redis
import pandas as pd
import aioredis
from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel


app = FastAPI()

NEWS_API_LIST_URL = "http://newsapi.org/v2/top-headlines?category=general&pageSize={limit}&page=1&apiKey={api_key}"
NEWS_API_SEARCH_URL = "http://newsapi.org/v2/everything?q={query}&pageSize={limit}&page=1&apiKey={api_key}"
API_KEY = "cd544524f61441d1982ecc8b3aaf9ca5"

REDDIT_BASE_URL = "https://www.reddit.com/r/news/top.json"
REDDIT_SECOND_BASE_URL = "https://www.reddit.com/r/news/search.json?q={query}&limit={limit}"

redis_conn = redis.Redis(host='localhost', port=6379, db=0)

@app.get("/fetch-data")
async def api_data(query: str = None, limit: int = 10):

    try:
        # Check if the data exists in Redis cache
        cache_key = f"cache:{query or 'default'}:{limit}"
        cached_data = redis_conn.get(cache_key)
        if cached_data:
            # If data exists in cache, return it directly
            return {"data": json.loads(cached_data)}

        # Set default parameters
        params = {
            "limit": limit,
            "apiKey": API_KEY,
        }
        if query:
            params["q"] = query  # Add the query parameter if provided
            async with httpx.AsyncClient() as client:
                reddit_response = await client.get(REDDIT_SECOND_BASE_URL, params=params)
                news_response = await client.get(NEWS_API_SEARCH_URL, params=params)
        else:
            async with httpx.AsyncClient() as client:
                reddit_response = await client.get(REDDIT_BASE_URL, params=params)
                news_response = await client.get(NEWS_API_LIST_URL, params=params)

        if reddit_response.status_code == 200:
            # Parse the JSON response
            reddit_data = reddit_response.json()
            reddit_raw_articles = reddit_data.get("data", {}).get("children", [])  # Extract the children list

            # Create a list of dictionaries containing the articles' data
            reddit_articles = [article["data"] for article in reddit_raw_articles]

            # Create a DataFrame from the articles
            reddit_articles_df = pd.DataFrame(reddit_articles)

            # Extract only the "title" and "url" columns
            reddit_articles_df = reddit_articles_df[["title", "url"]]

            # Add a "source" column with the value "reddit"
            reddit_articles_df["source"] = "reddit"

        if news_response.status_code == 200:
            # Parse the JSON response
            news_data = news_response.json()
            news_articles = news_data.get("articles", [])
            
            # Create a DataFrame from the articles
            news_articles_df = pd.DataFrame(news_articles)
            
            # Extract only the "title" and "url" columns
            news_articles_df = news_articles_df[["title", "url"]]

            # Add a "source" column with the value "reddit"
            news_articles_df["source"] = "newapi"
            
        final_df = pd.concat([news_articles_df, reddit_articles_df], axis=0,ignore_index=True)
        final_data_dict = final_df.to_dict(orient="records")
        if final_data_dict:
            # Store the data in Redis cache for future use
            redis_conn.set(cache_key, json.dumps(final_data_dict))
            redis_conn.expire(cache_key, 3600)
        return {"data": final_data_dict}
    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail="Unable to fetch data from the external API")
    
    