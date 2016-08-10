# -*- coding: utf-8 -*-
# Install TwitterSearch as it supports new API 1.1 . Do easy_install TwitterSearch
import sys
import time
import nltk
import re  
from time import strftime
from TwitterSearch import *
from couchdb.client import Server
from send_email import Send_Email
from django.utils.encoding import smart_str

# Set the search keywords and the database name here
search_keywords = ['kickstarter','kck.st']
database_name = 'twitter_kickstarter_search_new'

#Function to create/ return database instance
def Create_Couchdb_Instance(dtbs_name):
    
    server = Server("http://127.0.0.1:5984")
    try:
        db = server.create(dtbs_name)
    except Exception:
        db = server[dtbs_name]
    return db

def write_log(no_users_db,no_tweets_collected):
    f = open('log_'+database_name+'.txt','w')
    f.write('No of users :'+'\n')
    f.write(smart_str(no_users_db)+'\n')
    f.write('No of tweets'+'\n')
    f.write(smart_str(no_tweets_collected)+'\n')
    f.close()

class GetTwitterData:

    def Get_Data(self):
        
        MAX_PAGES = 15
        RESULTS_PER_PAGE = 100

        tso = TwitterSearchOrder() # create a TwitterSearchOrder object
        tso.setKeywords(search_keywords) # let's define all words we would like to have a look for
        tso.setLanguage('en') # we want to see German tweets only
        tso.setCount(RESULTS_PER_PAGE) # please dear Mr Twitter, only give us 1 results per page
        tso.setIncludeEntities(False) # and don't give us all those entity information

        # it's about time to create a TwitterSearch object with our secret tokens
        ts = TwitterSearch(
                           consumer_key = 'z',
                           consumer_secret = 'z',
                           access_token = '81498230-z',
                           access_token_secret = 'z'
                           )
        ts.authenticate() # we need to use the oauth authentication first to be able to sign messages
        i=0
        no_users = 0
        no_tweets = 0
        search_results = []
        twitter_dict = dict()

        #Retweet pattern to detect retweets
        rt_patterns = re.compile(r"(RT|via)((?:\b\W*@\w+)+)", re.IGNORECASE)   
        rt_origins_list = []
        rt_origins = ''
        
        db = Create_Couchdb_Instance(database_name)
        while(True):
            tso.setKeywords([search_keywords[i]])
            for tweet in ts.searchTweetsIterable(tso): # this is where the fun actually starts :)
                try:
                    user_id = tweet['user']['id_str']
                    tweet_text = tweet['text']
                    time_stmp = tweet['created_at']
                    try:
                        if 'media' in tweet['entities']:
                            if len(tweet['entities']['media'])>0:
                                im_id=tweet['entities']['media'][0]['id']
                                im_url=tweet['entities']['media'][0]['expanded_url']
                        if 'urls' in tweet['entities']:
                            if len(tweet['entities']['urls'])>0:
                                urls=tweet['entities']['urls']
                        if 'user_mentions' in tweet['entities']:
                            if len(tweet['entities']['user_mentions'])>0:
                                mentions=tweet['entities']['user_mentions']
                    except:
                        im_id=''
                        im_url=''
                        urls=''
                        mentions=''
                    # If screen name is not present , add all the details        
                    if user_id not in db:                                     
                        twitter_dict=dict()
                        twitter_dict['_id'] = user_id
                        twitter_dict['text']=[{'urls':urls,'image':[im_id,im_url],'mentions':mentions,'tweet_id':tweet['id'],'text':tweet_text,'timestamp':time_stmp,'coordinates':tweet['coordinates'],'source':tweet['source'],'in_reply_to_screen_name':tweet['in_reply_to_screen_name'],'retweet_count':tweet['retweet_count']}]
                        twitter_dict['friends_count'] = tweet['user']['friends_count']
                        twitter_dict['location'] = tweet['user']['location']
                        twitter_dict['profile_description'] = tweet['user']['description']
                        twitter_dict['tweet_count'] = tweet['user']['statuses_count']
                        twitter_dict['followers_count'] = tweet['user']['followers_count']
                        twitter_dict['screen_name'] = tweet['user']['screen_name']
                        twitter_dict['profile_created_at'] = tweet['user']['created_at']
                        no_users += 1
                        no_tweets += 1
                        db.create(twitter_dict)
                    # If screen name is present then check if the tweet is present otherwise append the tweet details
                    else:
                        doc = db[user_id]
                        tweets = {}
                        for tweet_dict in doc['text']:
                            tweets[tweet_dict['text']] = 'tweet'
                        if tweet_text not in tweets:
                            doc['text'].append({'urls':urls,'image':[im_id,im_url],'mentions':mentions,'tweet_id':tweet['id'],'text':tweet_text,'timestamp':time_stmp,'coordinates':tweet['coordinates'],'source':tweet['source'],'in_reply_to_screen_name':tweet['in_reply_to_screen_name'],'retweet_count':tweet['retweet_count']})
                            no_tweets += 1
                            db[user_id] = doc
                    print 'No of users :'
                    print no_users
                    print 'No of tweets :'
                    print no_tweets
                    print strftime("%Y-%m-%d %H:%M:%S")
                    write_log(no_users,no_tweets)
                except Exception as e:
                    print 'Error ...'
                    print e

            i = i+1
            # In the new API version 1.1 , the rate limits are fixed per 15 mins instead of per hour
            if i == len(search_keywords):
                i=0
                print "Sleeping for 15 mins"
                time.sleep(900)                                                      
        
if __name__ == '__main__':
    td = GetTwitterData()
    while(True):
        try:
            td.Get_Data()
        except Exception as e:
            if 'HTTPSConnectionPool' in e:
                time.sleep(2)
                continue
            else:
                message = 'Search API : '+database_name+' going to sleep with error : '+smart_str(e)
                print message
                Send_Email(message)
                time.sleep(1800)
    #search_api_new()
