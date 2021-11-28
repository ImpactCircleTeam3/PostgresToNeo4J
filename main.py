import os
import psycopg2
import csv
from typing import List, Optional
from datetime import datetime
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()


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
    language_code: str
    hashtags: List[str]
    tagged_persons: List[str]
    time_collected: datetime
    date_label: datetime


class Postgres:
    Postgres: Optional["Postgres"] = None

    @classmethod
    def get_instance(cls) -> "Postgres":
        if not cls.DB:
            cls.DB = cls()
        return cls.DB

    def __init__(self):
        self._connect()

    def _connect(self):
        self.conn = psycopg2.connect(**Settings.get_db_kwargs())
        self.cur = self.conn.cursor()


class ORM:
    db = Postgres.get_instance().Postgres

    @classmethod
    def fetch_latest_tweets(cls) -> List[Tweet]:
        latest_id = Settings.get_last_taken_id()
        sql = f"SELECT ({','.join(Tweet.__annotations__.keys())}) FROM tweet WHERE id > %s"
        cls.db.cur.execute(sql, (latest_id,))
        return [Tweet(*row) for row in cls.db.cur.fetchall()]


if __name__ == "__main__":
    print()
