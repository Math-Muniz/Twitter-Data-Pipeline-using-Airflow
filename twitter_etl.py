import tweepy
import pandas as pd
import json
from datetime import datetime
import s3fs

def run_twitter_etl():

    access_key = "" #API Key
    access_secret_key = "" #API Key Secret
    consumer_key = "" #Access Token
    consumer_secret_key = "" #Access Token Secret

    # Twitter authentication
    auth = tweepy.OAuthHandler(access_key, access_secret_key)
    auth.set_access_token(consumer_key, consumer_secret_key)

    # Creating an API object
    api = tweepy.API(auth)

    tweets = api.user_timeline(screen_name='@elonmusk',
                                #Username
                                count=200,
                                #200 is the maximum allowed count
                                include_rts = False,
                                # Necessary to keep full_text
                                # otherwise only the first 140 words are extracted
                                tweet_mode = 'extended'
                                )

    tweet_list = []
    for tweet in tweets:
        text = tweet._json["full_text"]

        refined_tweet = {'user' : tweet.user.screen_name,
                        'text' : text,
                        'favorite_count' : tweet.favorite_count,
                        'retweet_count' : tweet.retweet_count,
                        'created_at' : tweet.created_at
                        }

        tweet_list.append(refined_tweet)

    df = pd.DataFrame(tweet_list)
    df.to_csv("s3://math-airflow-twitter-bucket/refined_tweet.csv")