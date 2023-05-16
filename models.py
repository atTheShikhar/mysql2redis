from pydantic import BaseModel, BaseSettings


class Redis(BaseModel):
    host: str
    port: int = 6379
    user: str
    pwd: str


class MySQL(BaseModel):
    host: str
    db: str
    user: str
    pwd: str


class Creds(BaseSettings):
    mysql: MySQL
    redis: Redis

    class Config:
        env_file = ".env"
        env_file_decoding = "utf-8"
        env_nested_delimiter = "_"


class Table(BaseModel):
    mysql_table: str
    mysql_pk: str # primary key field
    redis_prefix: str # key prefix to use in redis


class RedisDocument(BaseModel):
    key: str
    val: dict
