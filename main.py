import os
import psycopg2
import csv
import sys
import logging
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
    def _exec_write_tweets_to_neo4j(cls, tx: Transaction, tweets: List[Tweet]):
        insert_tweet_queries = []
        insert_author_queries = []
        insert_author_relation_queries = []
        insert_hashtag_queries = []
        insert_relation_queries = []
        insert_linked_users_queries = []
        insert_linked_users_relation_queries = []

        for tweet_index, tweet in enumerate(tweets):
            insert_tweet_queries.append(
                """
                    MERGE (
                        t%i:Tweet {
                            id: %i,
                            text: '%s',
                            date: '%s',
                            favorite_count: %i,
                            retweet_count: %i
                        }
                    )
                """ % (
                    tweet_index,
                    tweet.status_id,
                    tweet.text.replace("'", " "),
                    tweet.date_label.strftime("%Y-%m-%d"),
                    tweet.favorite_count,
                    tweet.retweet_count
                )
            )

            insert_author_queries.append(
                "MERGE (u%i:User {name: '%s'})" % (
                    tweet_index,
                    tweet.author
                )
            )

            insert_author_relation_queries.append(
                "CREATE (u%i)<-[:WRITTEN_BY]-(t%i)" % (
                    tweet_index,
                    tweet_index
                )
            )

            for hashtag_index, hashtag in enumerate(tweet.hashtags):
                insert_hashtag_queries.append(
                    "MERGE (h%i_%i:Hashtag {hashtag: '%s'})" % (
                        tweet_index,
                        hashtag_index,
                        hashtag
                    )
                )
                insert_relation_queries.append(
                    "CREATE (t%i) <- [:IS_IN] - (h%i_%i)" % (
                        tweet_index,
                        tweet_index,
                        hashtag_index
                    )
                )

            for user_index, user in enumerate(tweet.tagged_persons):
                insert_hashtag_queries.append(
                    "MERGE (u%i_%i:User {name: '%s'})" % (
                        tweet_index,
                        user_index,
                        user
                    )
                )
                insert_relation_queries.append(
                    "CREATE (t%i) <- [:IS_LINKED_IN] - (u%i_%i)" % (
                        tweet_index,
                        tweet_index,
                        user_index
                    )
                )

        full_query_string = f"""
            {" ".join(insert_tweet_queries)}
            {" ".join(insert_author_queries)}
            {" ".join(insert_author_relation_queries)}
            {" ".join(insert_hashtag_queries)}
            {" ".join(insert_relation_queries)}
            {" ".join(insert_linked_users_queries)}
            {" ".join(insert_linked_users_relation_queries)}
        """

        tx.run(full_query_string)

    @classmethod
    def write_tweets_to_neo4j(cls, tweets: List[Tweet]):
        with cls.neo4j.driver.session() as session:
            session.write_transaction(cls._exec_write_tweets_to_neo4j, tweets)


def postgres_to_neo4j_connector():
    logger.info("Fetching latest tweets from database")
    tweets = ORM.fetch_latest_tweets()
    logger.info(f"Fetch Done. Fetched tweets: {len(tweets)}")
    for i in range(0, len(tweets) - 25, 25):
        logger.info(f"Insert tweets {i} to {i+25}")
        ORM.write_tweets_to_neo4j(tweets[i: i+25])
        ids = [tweet.id for tweet in tweets[i: i+25]]
        Settings.update_last_taken_id(max(ids))


if __name__ == "__main__":
    postgres_to_neo4j_connector()
    print()
