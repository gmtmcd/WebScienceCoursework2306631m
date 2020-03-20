"""
This program streams live tweets into a MongoDB filtered on hashtags
It extract from that database various statistics to determine the networking between them
Gemma McDonald
2306631m
"""
from tweepy import API 
from tweepy import Cursor
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import pymongo
from pymongo import MongoClient
import twitter_credentials
import json
from bson.json_util import dumps
from sklearn.feature_extraction.text import Tfidfvectoriser 
from sklearn.cluster import KMeans
from sklearn.metrics import adjusted_rand_score

all_data = []
rest_data = []

# THE TWITTER CLIENT #
class TwitterClient():
    # Authentication
    def __init__(self, twitter_user=None):
        self.auth = TwitterAuthenticator().authenticate_twitter_app()
        self.twitter_client = API(self.auth)
        self.twitter_user = twitter_user

    # Receives a number of tweets and will go add them to the MongoDB and the data list
    def get_user_timeline_tweets(self, num_tweets):
        tweets = []
        for tweet in Cursor(self.twitter_client.user_timeline, id=self.twitter_user).items(num_tweets):
            tweets.append(tweet)
            rest_data = tweet._json
            try:
                if rest_data['truncated'] == True:
                    all_data.append(rest_data['extended_tweet']['full_text'])
                else:
                    all_data.append(rest_data['text'])
                tweet_collection.insert_one(rest_data)
            except BaseException as err:
                #as this was a common error i removed this from being printed each time the code was run
                if err != 'extended_tweet':
                    print(err)
                return True
        return tweets

# THE TWITTER AUTHENTICATER #
class TwitterAuthenticator():
    # Authentication
    def authenticate_twitter_app(self):
        auth = OAuthHandler(twitter_credentials.CONSUMER_KEY, twitter_credentials.CONSUMER_SECRET)
        auth.set_access_token(twitter_credentials.ACCESS_TOKEN, twitter_credentials.ACCESS_TOKEN_SECRET)
        return auth

# THE TWITTER STREAMER #
"""
    Class to stream and process live tweets
"""
class TwitterStreamer():
    
    def __init__(self):
        self.twitter_autenticator = TwitterAuthenticator()    

    def stream_tweets(self, hash_tag_list):
        # Handles Twitter authentification and connection to Twitter Streaming API
        listener = TwitterListener()
        auth = self.twitter_autenticator.authenticate_twitter_app() 
        stream = Stream(auth, listener)

        # Filter Twitter Streams to capture data by the keywords and that are in English: 
        stream.filter(languages=["en"], track = hash_tag_list)


# THE TWITTER STREAM LISTENER #
"""
    Listener that prints received tweets to stdout
"""
class TwitterListener(StreamListener):
    
    def __init__(self, ):
        self.counter = 0
        super(TwitterListener,self).__init__()

    # Adds tweets to MongoDB and the data list
    def on_data(self, data):
        self.counter += 1
        try:
            if self.counter <= 50:
                tweet = json.loads(data)
                tweet_collection.insert_one(tweet)
                if tweet['truncated'] == True:
                    all_data.append(tweet['extended_tweet']['full_text'])
                else:
                    all_data.append(tweet['text'])
                return True
            else:
                return False
        except BaseException as e:
            print("Error on_data %s" % str(e))
        return True
          
    def on_error(self, status):
        if status == 420:
            return False
        print(status)

"""
Function to take a list of tweets and creates k clusters of groups
"""
def kclustering(text_list):
    vectoriser = Tfidfvectoriser(stop_words='english')
    X = vectoriser.fit_transform(text_list)
    true_k = int(round(0.1*(len(text_list))))
    model = KMeans(n_clusters=true_k, init='k-means++', max_iter=100, n_init=1)
    labels = model.fit_predict(X)
    print("Top terms per cluster:")
    order_centroids = model.cluster_centers_.argsort()[:,::-1]
    terms = vectoriser.get_feature_names()
    for i in range(true_k):
        print("Cluster %d: " % i,)
        for ind in order_centroids[i,:10]:
            print(' %s' % terms[ind],)
            print(" ")
    return labels


"""
Function to account for a new interaction between two users
It ensures that the dictionaries used to record the interaction are created
"""
def newInteraction(tieDictionary, tie1, tie2):
    newDictionary = {'hashtags': 0, 'mentionedAtThem': 0, 'mentionedByThem': 0, 'mentionedSamePerson': 0, 'repliedToThem': 0, 'repliedToByThem': 0, 'retweetedByThem': 0, 'retweetedTheirTweet': 0, 'quotedByThem': 0, 'quotedAtThem': 0}
    if tie1 not in tieDictionary:
        tieDictionary[tie1] = {}
    if tie2 not in tieDictionary[tie1]:
        tieDictionary[tie1][tie2] = newDictionary
    if tie2 not in tieDictionary:
        tieDictionary[tie2] = {}
    if tie1 not in tieDictionary[tie2]:
        tieDictionary[tie2][tie1] = newDictionary


"""
Function to find and store the networks and cluster networks
of the data
"""
def networks(tweetList):
    tieDictionary = {}
    triadsList = []
    totalMentionDictionary = {}
    networkInteractions = {'hashtags': 0, 'mentions': 0, 'replies': 0, 'retweets': 0, 'quotes': 0}
    hashtagInteractions = {}

    # create loop through list of tweets
    for outsideTweet in tweetList:
        outsideHashtags = outsideTweet['entities']['hashtags']
        outsideMentions = outsideTweet['entities']['user_mentions']
        outsideUser = outsideTweet['user']['id']

        # get list of every mention within tweet
        for mention in outsideMentions:
            if mention['screen_name'] in totalMentionDictionary:
                totalMentionDictionary[mention['screen_name']] += 1
            else:
                totalMentionDictionary[mention['screen_name']] = 1
        
        # nested loop to find ties
        # goes through every tweet to check against every other tweet to see if theres a tie
        for insideTweet in tweetList:
            insideUser = insideTweet['user']['id']
            if (insideUser != outsideUser):
                insideHashtags = insideTweet['entities']['hashtags']
                insideMentions = insideTweet['entities']['user_mentions']

                # check for hashtag type ties
                for eachOutsideHashtag in outsideHashtags:
                    for eachInsideHashtag in insideHashtags:
                        if eachInsideHashtag['text'] not in hashtagInteractions:
                            # interactedWith prevents double counting hashtags when it finds reverse of previously known tie
                            hashtagInteractions[eachInsideHashtag['text']] = {'interactedWith': []}
                        if eachOutsideHashtag['text'] not in hashtagInteractions: 
                            hashtagInteractions[eachOutsideHashtag['text']] = {'interactedWith': []}
                        if eachInsideHashtag['text'] not in hashtagInteractions[eachOutsideHashtag['text']] and insideTweet not in hashtagInteractions[eachOutsideHashtag['text']]['interactedWith']:
                            hashtagInteractions[eachOutsideHashtag['text']][eachInsideHashtag['text']] = 0
                        if eachOutsideHashtag['text'] not in hashtagInteractions[eachInsideHashtag['text']] and outsideTweet not in hashtagInteractions[eachInsideHashtag['text']]['interactedWith']:
                            hashtagInteractions[eachInsideHashtag['text']][eachOutsideHashtag['text']] = 0
                        if outsideTweet['id'] not in hashtagInteractions[eachInsideHashtag['text']]['interactedWith']:
                            hashtagInteractions[eachInsideHashtag['text']][eachOutsideHashtag['text']] += 1
                            hashtagInteractions[eachInsideHashtag['text']]['interactedWith'].append(outsideTweet['id'])
                        if insideTweet['id'] not in hashtagInteractions[eachOutsideHashtag['text']]['interactedWith']:
                            hashtagInteractions[eachOutsideHashtag['text']][eachInsideHashtag['text']] += 1
                            hashtagInteractions[eachOutsideHashtag['text']]['interactedWith'].append(insideTweet['id'])
                        if eachInsideHashtag['text'] == eachOutsideHashtag['text']:
                            newInteraction(tieDictionary, insideUser, outsideUser)
                            tieDictionary[outsideUser][insideUser]['hashtags'] += 1
                            tieDictionary[insideUser][outsideUser]['hashtags'] += 1
                            networkInteractions['hashtags'] += 1
                # find the ties involving mentions
                for eachOutsideMention in outsideMentions:
                    for eachInsideMention in insideMentions:
                        if eachInsideMention['id'] == eachOutsideMention['id']:
                            newInteraction(tieDictionary, insideUser, outsideUser)
                            tieDictionary[outsideUser][insideUser]['mentionedSamePerson'] += 1
                            tieDictionary[insideUser][outsideUser]['mentionedSamePerson'] += 1
                        if eachInsideMention['id'] == outsideUser:
                            newInteraction(tieDictionary, insideUser, outsideUser)
                            tieDictionary[outsideUser][insideUser]['mentionedByThem'] += 1
                            tieDictionary[insideUser][outsideUser]['mentionedAtThem'] += 1
                            networkInteractions['mentions'] += 1
        # checks to see if tweet is a reply
        if outsideTweet['in_reply_to_status_id'] != None:
            replyStatus = outsideTweet['in_reply_to_user_id']
            newInteraction(tieDictionary, replyStatus, outsideUser)
            tieDictionary[replyStatus][outsideUser]['repliedToByThem'] += 1
            tieDictionary[outsideUser][replyStatus]['repliedToThem'] += 1
            networkInteractions['replies'] += 1
        # if no retweeted status then set equal to fail
        if outsideTweet.get('retweeted_status', "fail") != "fail":
            retweetStatus = outsideTweet['retweeted_status']['user']['id']
            newInteraction(tieDictionary, retweetStatus, outsideUser)
            tieDictionary[retweetStatus][outsideUser]['retweetedByThem'] += 1
            tieDictionary[outsideUser][retweetStatus]['retweetedTheirTweet'] += 1
            networkInteractions['retweets'] += 1
        # if no quoted status then set equal to fail
        if outsideTweet.get('quoted_status', "fail") != "fail":
            quoteStatus = outsideTweet['quoted_status']['user']['id']
            newInteraction(tieDictionary, quoteStatus, outsideUser)
            tieDictionary[quoteStatus][outsideUser]['quotedByThem'] += 1
            tieDictionary[outsideUser][quoteStatus]['quotedAtThem'] += 1
            networkInteractions['quotes'] += 1

    # Check for triads, ie check for the third participant in the path
    for levelOneTie in tieDictionary:
        for levelTwoTie in tieDictionary[levelOneTie]:
            for levelThreeTie in  tieDictionary[levelTwoTie]:
                if levelOneTie != levelThreeTie:
                    if set([levelOneTie,levelTwoTie,levelThreeTie]) not in triadsList:
                        triadsList.append(set([levelOneTie, levelTwoTie, levelThreeTie]))

    return tieDictionary, triadsList, totalMentionDictionary, networkInteractions, hashtagInteractions

 
"""
Main function to collect the tweets, gather the extra tweets based off of the original tweets

"""
if __name__ == '__main__':

    # Set up connection with the Mongo Database
    client = MongoClient()
    db = client.TweetDB
    tweet_collection = db.CovidTweets

    # Clear the old tweets from the database
    try: 
        tweet_collection.drop()
    except:
        pass
 
    # Specify the hashtags to be used and stream the tweets
    hash_tag_list = ["corona virus", "covid-19", "coronavirus"]
    # #fetched_tweets_filename = "tweets.txt"
    twitter_streamer = TwitterStreamer()
    twitter_streamer.stream_tweets(hash_tag_list)

    # gets tweets from the mongoDB
    databaseData = tweet_collection.find()
    JSONinfo = dumps(databaseData)
    tweetStream = json.loads(JSONinfo)
    print(len(tweetStream))
    

    # code here fixes an error occuring where the user value is not accepted
    # instead fills it with a dummy value
    # it creates an instance of the twitter client and calls the function
    # to get 10 tweets per tweet user
    tempUser = ""
    for record in tweetStream:
        try:
            twitter_client = TwitterClient(record['user']['screen_name'])
        except:
            if tempUser != "":
                twitter_client = TwitterClient(tempUser['screen_name'])
            else:
                continue
        if tempUser == "":
            tempUser = record['user']
        twitter_client.get_user_timeline_tweets(10)
    
    databaseData = tweet_collection.find()
    JSONinfo = dumps(databaseData)
    tweetStream = json.loads(JSONinfo)
    clusters = kclustering(all_data)
    
    
    tweetClusters = {}
    tieClusters = {}
    triadClusters = {}
    mentionClusters = {}
    typeClusters = {}
    listClusters = []
    hashtagClusters = {}

    # empty all clustured data
    for i in range(len(tweetStream) - 1):
        if clusters[i] not in tweetClusters:
            tweetClusters[clusters[i]] = []
            tieClusters[clusters[i]] = {}
            triadClusters[clusters[i]] = []
            mentionClusters[clusters[i]] = {}
            typeClusters[clusters[i]] = {}
            hashtagClusters[clusters[i]] = {}
            if clusters[i] not in listClusters:
                listClusters.append(clusters[i])
        tweetClusters[clusters[i]].append(tweetStream[i])
    
    # set up the cluster dictionary
    clusterDict = {'tweets': tweetClusters, 'ties': tieClusters, 'triads': triadClusters, 'mentions': mentionClusters, 'types': typeClusters, 'hashtags': hashtagClusters}
    totalNetworks = {'ties': {}, 'triads': [], 'mentions': {}, 'types': {}, 'hashtags': {}}
    totalNetworks['ties'], totalNetworks['triads'], totalNetworks['mentions'], totalNetworks['types'], totalNetworks['hashtags'] = networks(tweetStream)
    print(clusterDict.keys())

    # fill in cluster dictionarys
    for cluster in listClusters:
        clusterDict['ties'][cluster], clusterDict['triads'][cluster], clusterDict['mentions'][cluster], clusterDict['types'][cluster], clusterDict['hashtags'][cluster] = networks(clusterDict['tweets'][cluster])
    


    # prints the stats of the tweets and the clusters
    print("TOTAL TWEETS")
    for net_type in totalNetworks:
        print(str(net_type) + " " + str(len(totalNetworks[net_type])))
    

    for cluster in listClusters:
        print("For cluster " + str(cluster))
        for net_type in clusterDict:
            if net_type != "tweets":
                print(str(net_type) + " " + str(len(clusterDict[net_type][cluster])))
            print("Ties: " + str(len(clusterDict["ties"][cluster])))
            print("Triads: " + str(len(clusterDict["triads"][cluster])))
            print("Mentions: " + str(clusterDict["mentions"][cluster]))
            print("Types: " + str(clusterDict["types"][cluster]))
            print("Hashtags: " + str(clusterDict["hashtags"][cluster]))
        print("\n")
        print("\n")
        print("\n")
        print("\n")

    print("TWEETS: " + str(len(tweetStream)))

    """
    This code appears to work, but, produces an immense amount of data
    output, i therefore do not know if there is a fundamental error in the
    way i have coded this section, or due to the immense understandable 
    interest in covid virus - discussed in report
    """
    # print("Total ties dict: " + str(len(totalNetworks['ties'])))
    # print("Total triads dict: " + str(len(totalNetworks['triads'])))
    # print("Total mentions dict: " + str(totalNetworks['mentions']))
    # print("Total types dict: " + str(totalNetworks['types']))
    # print("Total hashtags dict: " + str(totalNetworks['hashtags'][0]))


    # counter to find the number of clusters, the max, min and avg cluster sizes
    totalClust = 0
    totalClustTweets = 0
    avgClustSize = 0
    minClust = 100000
    maxClust = 0
    for cluster in listClusters:
        totalClust += 1
        counter = 0
        for tweet in clusterDict['tweets'][cluster]:
            totalClustTweets += 1
            counter += 1
        if counter < minClust:
            minClust = counter
        if counter > maxClust:
            maxClust = counter
    avgClustSize = totalClustTweets / totalClust
    print("Max cluster: " + str(maxClust) + "\nMin cluster: " + str(minClust) + "\navgClusterSize: " + str(avgClustSize))
    