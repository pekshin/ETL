import csv
import json
import sqlite3
import time

import pandas as pd

path = 'C://Users//pekhtdx//OneDrive//Python//ETL//three_minutes_tweets.json.txt'
database = "C://Users//pekhtdx//OneDrive//Python//ETL//twitter.db"
text_sentiment = "C://Users//pekhtdx//OneDrive//Python//ETL//AFINN-111.txt"
# сразу наполню массив данными.
tweets = []
for line in open(path, 'r'):
    tweets.append(json.loads(line))

# и словарик по text_sentiment
word = []
mark = []
# создадим словарик для удобства
with open(text_sentiment) as f:
    reader = csv.reader(f, delimiter="\t")
    for i in list(reader):
        word.append(i[0])
        mark.append(i[1])
text_sentiment_dict = dict(zip(word, mark))

#-------------------------------------------------FUNCTIONS-------------------------------------------------------------
def sentiment_it(string):
    """
    Find sentiming of each post
    :param string
    :return: tweet_sentiment_sum
    """
    #     spisok = string.split ()
    tweet_sentiment_sum = 0
    for i in string:
        if i in text_sentiment_dict:
            tweet_sentiment_sum += int(text_sentiment_dict[i])
    return (tweet_sentiment_sum)


def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)

    return None


def insert_values(conn, data):
    """
    Create a new project into the projects table
    :param conn:
    :param data:
    :return: sql_insert
    """
    sql = u''' INSERT INTO twitter_table(name,tweet_text,country_code,lang,created_at, location, display_url, text_sentiment)
              VALUES(?,?,?,?,?,?,?,?)'''
    cur = conn.cursor()
    cur.execute(sql, data)
    return cur.lastrowid




#----------------------------------------------------BODY---------------------------------------------------------------

while True:
    name = []
    tweet_text = []
    country_code = []
    display_url = []
    lang = []
    created_at = []
    location = []
    for i in range(1, len(tweets)):

        if 'delete' not in tweets[i]:
            name.append(tweets[i]['user']['name'])
            tweet_text.append(tweets[i]['text'])
            lang.append(tweets[i]['lang'])
            created_at.append(tweets[i]['created_at'])
            location.append(tweets[i]['user']['location'])
            if not tweets[i]['entities']['urls']:
                display_url.append("None")
            else:
                display_url.append(tweets[i]['entities']['urls'][0]['display_url'])

            if not tweets[i]['place']:
                country_code.append("None")
            else:
                country_code.append(tweets[i]['place']["country_code"])

    dictio = {'name': name,
              'tweet_text': tweet_text,
              'country_code': country_code,
              'lang': lang,
              'created_at': created_at,
              'location': location,
              'display_url': display_url,

              }
    df1 = pd.DataFrame.from_dict(dictio)

    text_sentiment_column = df1['tweet_text'].str.split(' ').apply(sentiment_it)
    df1["text_sentiment"] = text_sentiment_column
    conn = create_connection(database)

    with conn:
        for index, row in df1.iterrows():
            twitter_table = insert_values(conn, [row['name'],
                                                 row['tweet_text'],
                                                 row['country_code'],
                                                 row['lang'],
                                                 row['created_at'],
                                                 row['location'],
                                                 row['display_url'],
                                                 row['text_sentiment']])
    print('wait for new rows')
    time.sleep(180) # every three minutes update
