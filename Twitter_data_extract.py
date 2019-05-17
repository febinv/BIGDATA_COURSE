import tweepy
import time
import pandas as pd
from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
CONSUMER_KEY = ''
CONSUMER_SECRET = ''
ACCESS_KEY = ''
ACCESS_SECRET = ''

data = pd.read_csv("Twitter_top100leaders_friends_det_3.csv")

class TweetListener(StreamListener):
    # A listener handles tweets are the received from the stream.
    #This is a basic listener that just prints received tweets to standard output

    def on_data(self, data):
        print(data)
        return True

    def on_error(self, status):
        print(status)



auth = OAuthHandler(CONSUMER_KEY,CONSUMER_SECRET)
api = tweepy.API(auth)

auth.set_access_token(ACCESS_KEY, ACCESS_SECRET)
twitterStream = Stream(auth,TweetListener())

final_data=pd.DataFrame()

for idx, row in data.iterrows():
    print(data.loc[idx,'user_name'])
    final_data_friends=pd.DataFrame()
    list_user=[]
    list_friends=[]
    count=0
    #user = api.get_user(data.loc[idx,'user_name'])
    try:
        friends = api.friends_ids(data.loc[idx,'user_name'])
    except:
        time.sleep(16*60)
        continue
    if friends:
        for idx1,word in enumerate(friends):
                try:
                    if count <=50:
                        user_friends = api.get_user(word)
                        print(user_friends)
                        list_friends.append([data.loc[idx,'user_name']]+[user_friends.id]+[user_friends.screen_name]+[user_friends.description]+[user_friends.friends_count]+[user_friends.followers_count]+[user_friends.location]+[user_friends.url])
                        count+=1
                    else:
                        continue
                except:
                 time.sleep(16*60)
                 continue
        #list_user.append([user.id]+[user.screen_name]+[user.description]+[user.friends_count]+[user.followers_count]+[user.location]+[user.url])
        #final_data=final_data.append(list_user)
    else:
           list_friends.append([data.loc[idx,'user_name']])
    final_data_friends=final_data_friends.append(list_friends)
    final_data_friends.to_csv("Twitter_top100leaders_friends_friends_profile_latest_new_2_firstthread.csv",encoding='utf-8', mode='a')
#final_data.to_csv("Twitter_top100leaders_profile.csv",encoding='utf-8')

