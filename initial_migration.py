import os
import psycopg2
import csv
import sys
import logging
import functools
import operator
from neo4j import GraphDatabase
from typing import List, Optional
from datetime import datetime
from dataclasses import dataclass
from dotenv import load_dotenv
from neo4j.work.transaction import Transaction

load_dotenv()

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
fmt = logging.Formatter("%(asctime)s %(levelname)-8s %(name)-30s %(message)s")
sh = logging.StreamHandler(sys.stderr)
sh.setFormatter(fmt)
logger.addHandler(sh)


class Settings:
    BASE_DIR = os.path.dirname(os.path.realpath(__file__))

    @staticmethod
    def get_db_kwargs() -> dict:
        return {
            "user": os.getenv("DB_USER"),
            "password": os.getenv("DB_PASSWORD"),
            "host": os.getenv("DB_HOST"),
            "port": int(os.getenv("DB_PORT")),
            "database": os.getenv("DB_NAME")
        }

    @staticmethod
    def get_neo4j_kwargs() -> dict:
        return {
            "user": os.getenv("NEO4J_USER"),
            "password": os.getenv("NEO4J_PASSWORD"),
            "uri": os.getenv("NEO4J_URI")
        }

    @classmethod
    def get_last_taken_id(cls) -> int:
        data_path = os.path.join(cls.BASE_DIR, "cache.csv")
        with open(data_path, mode="r") as csv_file:
            reader = csv.reader(csv_file)
            last_taken_id = int(next(reader)[1])
        return last_taken_id

    @classmethod
    def update_last_taken_id(cls, new_value: int):
        data_path = os.path.join(cls.BASE_DIR, "cache.csv")
        with open(data_path, mode="w") as file:
            file.write(f"last_taken_id,{new_value}")


@dataclass
class Tweet:
    id: int
    status_id: int
    text: str
    url: str
    favorite_count: int
    retweet_count: int
    trend: str
    normalized_trend: str
    author: str
    language_code: str
    hashtags: List[str]
    tagged_persons: List[str]
    time_collected: datetime
    date_label: datetime


class Postgres:
    Postgres: Optional["Postgres"] = None

    @classmethod
    def get_instance(cls) -> "Postgres":
        if not cls.Postgres:
            cls.Postgres = cls()
        return cls.Postgres

    def __init__(self):
        self._connect()

    def _connect(self):
        self.conn = psycopg2.connect(**Settings.get_db_kwargs())
        self.cur = self.conn.cursor()


class Neo4J:
    Neo4J: Optional["Neo4J"] = None

    @classmethod
    def get_instance(cls) -> "Neo4J":
        if not cls.Neo4J:
            cls.Neo4J = cls()
        return cls.Neo4J

    def __init__(self):
        self._connect()

    def _connect(self):
        neo4j_kwargs = Settings.get_neo4j_kwargs()
        self.driver = GraphDatabase.driver(
            neo4j_kwargs["uri"],
            auth=(neo4j_kwargs["user"], neo4j_kwargs["password"])
        )


class ORM:
    pg_db = Postgres.get_instance().Postgres
    neo4j = Neo4J.get_instance().Neo4J

    @classmethod
    def fetch_latest_tweets(cls) -> List[Tweet]:
        latest_id = Settings.get_last_taken_id()
        sql = f"SELECT {','.join(Tweet.__annotations__.keys())} FROM tweet WHERE id > %s"
        cls.pg_db.cur.execute(sql, (latest_id,))
        return [Tweet(*row) for row in cls.pg_db.cur.fetchall()]

    @classmethod
    def get_unique_users(cls) -> List[str]:
        author_sql = "SELECT DISTINCT(author) FROM tweet"
        cls.pg_db.cur.execute(author_sql)
        author_list = [entity[0] for entity in cls.pg_db.cur.fetchall()]

        linked_users = "SELECT tagged_persons FROM tweet"
        cls.pg_db.cur.execute(linked_users)
        linked_users_list = [entity[0] for entity in cls.pg_db.cur.fetchall() if entity != []]

        # flatten list
        linked_users_list = list(functools.reduce(operator.concat, linked_users_list))

        # create unique lists of users
        user_list = list(set(author_list + linked_users_list))

        return user_list

    @classmethod
    def get_unique_hashtags(cls) -> List[str]:
        hashtag_sql = "SELECT hashtags FROM tweet"
        cls.pg_db.cur.execute(hashtag_sql)
        hashtag_list = [entity[0] for entity in cls.pg_db.cur.fetchall() if entity != []]

        # flatten list
        hashtag_list = list(functools.reduce(operator.concat, hashtag_list))

        # create unique lists of hashtags
        hashtag_list = list(set(hashtag_list))

        return hashtag_list

    @classmethod
    def _exec_write_tweets_to_neo4j(cls, tx: Transaction, tweet: Tweet):
        create_tweet_query = """
            CREATE (t:Tweet {
                id: %i,
                text: '%s',
                date: '%s',
                favorite_count: %i,
                retweet_count: %i
            })
        """ % (
            tweet.status_id,
            tweet.text.replace("'", " "),
            tweet.date_label.strftime("%Y-%m-%d"),
            tweet.favorite_count,
            tweet.retweet_count
        )
        tx.run(create_tweet_query)

        insert_author_query = """
            MATCH
                (t:Tweet),
                (u:User)
            WHERE t.id=%i AND u.username='%s'
            CREATE (t)-[:IS_WRITTEN_BY]->(u)
        """ % (tweet.status_id, tweet.author)
        tx.run(insert_author_query)

        for hashtag in tweet.hashtags:
            insert_hashtag_query = """
                MATCH
                    (t:Tweet),
                    (h:Hashtag)
                WHERE t.id=%i AND h.hashtag='%s'
                CREATE (h)-[:IS_IN]->(t)
            """ % (tweet.status_id, hashtag)
            tx.run(insert_hashtag_query)

        for user in tweet.tagged_persons:
            insert_linked_user_query = """
                MATCH
                    (t:Tweet),
                    (u:User)
                WHERE t.id=%i AND u.username='%s'
                CREATE (u)-[:IS_MENTIONED_IN]->(t)
            """ % (tweet.status_id, user)
            tx.run(insert_linked_user_query)

    @classmethod
    def write_tweets_to_neo4j(cls, tweets: List[Tweet]):
        with cls.neo4j.driver.session() as session:
            for tweet in tweets:
                session.write_transaction(cls._exec_write_tweets_to_neo4j, tweet)

    @classmethod
    def _exec_write_users_to_neo4j(cls, tx: Transaction, users: List[str]):
        user_nodes = [
            "(u%i:User {username: '%s'})" % (i, user)
            for i, user in enumerate(users)
        ]
        query = f"CREATE {','.join(user_nodes)}"
        tx.run(query)

    @classmethod
    def _exec_write_hashtags_to_neo4j(cls, tx: Transaction, hashtags: List[str]):
        hashtag_nodes = [
            "(h%i:Hashtag {hashtag: '%s'})" % (i, hashtag)
            for i, hashtag in enumerate(hashtags)
        ]
        query = f"CREATE {','.join(hashtag_nodes)}"
        tx.run(query)

    @classmethod
    def write_users_to_neo4j(cls, users: List[str]):
        with cls.neo4j.driver.session() as session:
            session.write_transaction(cls._exec_write_users_to_neo4j, users)

    @classmethod
    def write_hashtags_to_neo4j(cls, hashtags: List[str]):
        with cls.neo4j.driver.session() as session:
            session.write_transaction(cls._exec_write_hashtags_to_neo4j, hashtags)


def postgres_to_neo4j_connector():
    logger.info("Fetch Unique Users from database")
    users = ORM.get_unique_users()
    logger.info(f"Start writing {len(users)} Users to neo4j")
    ORM.write_users_to_neo4j(users)

    logger.info("Fetch Unique Hashtags from database")
    hashtags = ORM.get_unique_hashtags()
    logger.info(f"Start writing {len(hashtags)} Users to neo4j")
    ORM.write_hashtags_to_neo4j(hashtags)

    logger.info("Fetching latest tweets from database")
    tweets = ORM.fetch_latest_tweets()
    logger.info(f"Fetch Done. Fetched tweets: {len(tweets)}")
    for i in range(0, len(tweets) - 10, 10):
        logger.info(f"Insert tweets {i} to {i+10}")
        ORM.write_tweets_to_neo4j(tweets[i: i+10])
        ids = [tweet.id for tweet in tweets[i: i+10]]
        Settings.update_last_taken_id(max(ids))


if __name__ == "__main__":
    postgres_to_neo4j_connector()
    print()
