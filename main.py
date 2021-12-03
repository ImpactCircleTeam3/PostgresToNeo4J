import os
import psycopg2
import sys
import logging
import uuid
from time import sleep
from neo4j import GraphDatabase
from typing import List, Optional, Callable
from datetime import datetime
from dataclasses import dataclass
from dotenv import load_dotenv
from neo4j.work.transaction import Transaction
from neo4j.exceptions import ConstraintError
from confluent_kafka import Consumer

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

    @staticmethod
    def get_kafka_consumer_kwargs() -> dict:
        return {
            "bootstrap.servers": os.getenv("KAFKA_CLUSTER_SERVER"),
            "group.id": f"pg2neo-{uuid.uuid4().hex[:6]}",
            'auto.offset.reset': 'earliest',
            'security.protocol': 'SASL_SSL',
            'sasl.mechanisms': 'PLAIN',
            'sasl.username': os.getenv("KAFKA_CLUSTER_API_KEY"),
            'sasl.password': os.getenv("KAFKA_CLUSTER_API_SECRET")
        }


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


@dataclass
class Hashtag:
    id: int
    hashtag: str


@dataclass
class TwitterUser:
    id: int
    username: str


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
        self.conn.autocommit = True
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

    @classmethod
    def exec(cls, executable: Callable, **kwargs):
        neo4j = cls.get_instance().Neo4J
        with neo4j.driver.session() as session:
            session.write_transaction(executable, **kwargs)


class KafkaConnector:
    kafka_connector: Optional["KafkaConnector"] = None

    @classmethod
    def get_instance(cls) -> "KafkaConnector":
        if not cls.kafka_connector:
            cls.kafka_connector = cls()
        return cls.kafka_connector

    def __init__(self):
        self.consumer = Consumer(
            Settings.get_kafka_consumer_kwargs()
        )


class ORM:
    pg_db = Postgres.get_instance().Postgres

    @classmethod
    def get_index_from_pg(cls, key: str) -> int:
        sql = "SELECT value FROM postgres_to_neo_cache WHERE key=%s"
        cls.pg_db.cur.execute(sql, (key,))
        return cls.pg_db.cur.fetchone()[0]

    @classmethod
    def update_index_in_pg(cls, key: str, index: int):
        sql = "UPDATE postgres_to_neo_cache SET value=%s WHERE key=%s"
        cls.pg_db.cur.execute(sql, (index, key,))

    @classmethod
    def fetch_latest_tweets_ordered_by_id(cls) -> List[Tweet]:
        latest_id = cls.get_index_from_pg(key="last_taken_id")
        sql = f"SELECT {','.join(Tweet.__annotations__.keys())} FROM tweet WHERE id > %s ORDER BY id"
        cls.pg_db.cur.execute(sql, (latest_id,))
        return [Tweet(*row) for row in cls.pg_db.cur.fetchall()]

    @classmethod
    def fetch_latest_hashtags(cls) -> List[Hashtag]:
        latest_hashtag_id = cls.get_index_from_pg(key="last_taken_hashtag_id")
        logger.info(f"Start Fetching latest hashtags. Latest index: {latest_hashtag_id}")
        sql = f"SELECT {','.join(Hashtag.__annotations__.keys())} FROM hashtag WHERE id > %s ORDER BY id"
        cls.pg_db.cur.execute(sql, (latest_hashtag_id,))
        hashtags = [Hashtag(*row) for row in cls.pg_db.cur.fetchall()]
        logger.info(f"Fetched hashtags: {len(hashtags)}")
        return hashtags

    @classmethod
    def fetch_latest_users(cls) -> List[TwitterUser]:
        latest_user_id = cls.get_index_from_pg(key="last_taken_twitter_user_id")
        logger.info(f"Start Fetching latest users. Latest index: {latest_user_id}")
        sql = f"SELECT {','.join(TwitterUser.__annotations__.keys())} FROM twitter_user WHERE id > %s ORDER BY id"
        cls.pg_db.cur.execute(sql, (latest_user_id,))
        users = [TwitterUser(*row) for row in cls.pg_db.cur.fetchall()]
        logger.info(f"Fetched Users: {len(users)}")
        return users

    @classmethod
    def write_hashtags_to_neo4j(cls, tx: Transaction, hashtag: Hashtag):
        try:
            tx.run("CREATE (h:Hashtag {hashtag: '%s'})" % hashtag.hashtag)
        except ConstraintError:
            logger.warning(f"Hashtag w. tag {hashtag.hashtag} already exists")

    @classmethod
    def write_users_to_neo4j(cls, tx: Transaction, user: TwitterUser):
        try:
            tx.run("CREATE (u:User {username: '%s'})" % user.username)
        except ConstraintError:
            logger.warning(f"User w. username {user.username} already exists")

    @classmethod
    def write_tweet_to_neo4j(cls, tx: Transaction, tweet: Tweet, index: int, max_index: int):
        logger.info(f"Start writing Tweet {index+1}/{max_index} w. id {tweet.status_id} to Neo4J")
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


def runner():
    logger.info("Start Runner")

    users = ORM.fetch_latest_users()
    for i, user in enumerate(users):
        logger.info(f"Write User {i + 1} / {len(users)} w. usernamme {user.username} to Neo4J")
        Neo4J.exec(ORM.write_users_to_neo4j, user=user)
        ORM.update_index_in_pg(key="last_taken_twitter_user_id", index=user.id)

    hashtags = ORM.fetch_latest_hashtags()
    for i, hashtag in enumerate(hashtags):
        logger.info(f"Write Hashtag {i + 1} / {len(hashtags)} w. tag {hashtag.hashtag} to Neo4J")
        Neo4J.exec(ORM.write_hashtags_to_neo4j, hashtag=hashtag)
        ORM.update_index_in_pg(key="last_taken_hashtag_id", index=hashtag.id)

    tweets = ORM.fetch_latest_tweets_ordered_by_id()
    for i, tweet in enumerate(tweets):
        Neo4J.exec(ORM.write_tweet_to_neo4j, tweet=tweet, index=i, max_index=len(tweets))
        ORM.update_index_in_pg(key="last_taken_id", index=tweet.id)
    logger.info("Done.")


def event_listener():
    consumer = KafkaConnector.get_instance().kafka_connector.consumer
    consumer.subscribe(["pg-sync"])
    while True:
        msg = consumer.poll(timeout=1)
        if msg is None:
            logger.info("No Message Received. Wait for polling.")
            continue
        elif msg.error():
            logger.error(msg.error())
        else:
            runner()


if __name__ == "__main__":
    while True:
        try:
            event_listener()
        except Exception as e:
            logger.exception(str(e))
        logger.info("Wait for reconnection")
        sleep(60)
