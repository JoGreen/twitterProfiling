import tweepy

class UserInfo:
    consumer_key = 'DDI8q1kk7MWY7R9tTn42r9ZEO'
    consumer_secret = 'awRFoif9O4PrAzgzQMsaOfngTgQnCHVoHa5RlJmWgjVjDEKuUI'
    access_token = '850336176364023808-1Cdr4gtp0oIIS6LKrIWujzDnO620VKf'
    access_token_secret = 'Shru0b5KLXXuQjIL3nzj3NaR0M3TCiKkZLyHT1zjootKM'

    def __init__(self):
        auth = tweepy.OAuthHandler(UserInfo.consumer_key, UserInfo.consumer_secret)
        auth.set_access_token(UserInfo.access_token, UserInfo.access_token_secret)
        self.api = tweepy.API(auth)

    def get_friends(self, user_id):
        friends_ids = []

        try:
            friends_ids = self.api.friends_ids(user_id)
            friends_ids  = map(str, friends_ids)

        except Exception as e:
            print(e)

        finally:
            print(friends_ids)
            return friends_ids

    def get_retweets(self, user_id):
        #status = self.api.rate_limit_status()
        retweets = self.api.user_timeline(user_id, None, None, 1)
        map(show, retweets)

def show(doc):
    print doc

#UserInfo().get_retweets("30524827")