import redis, logging, json
from json import JSONEncoder
from redis import Redis

from models import RedisDocument


class RedisActions():
    def __init__(self, host: str, password: str, user: str = "default", port: int = 6379) -> None:
        self.pipe = None
        try:
            self.redis_client: Redis = redis.Redis(
                host=host,
                password=password,
                port=port,
                username=user,
                db=0,
                socket_connect_timeout=20 # timeout if connection isn't establised within 20 sec
            )
            self.ping()
        except Exception as e:
            logging.error(f"redis error: {e}")
            self.redis_client = None

    def _redis_avl(func):
        def inner(self, *args, **kwargs):
            if not self.redis_client:
                logging.error(f"redis error: client not initiated!")
                return
            try:
                return func(self, *args, **kwargs)
            except Exception as e:
                logging.error(f"redis error: {e}")
                return
        return inner

    def _pipelined(func):
        def inner(self, *args, **kwargs):
            if not self.redis_client:
                logging.error(f"redis error: client not initiated!")
                return
            try:
                if not self.pipe:
                    self.pipe = self.redis_client.pipeline()
                func(self, *args, **kwargs)
                self.pipe.execute()
            except Exception as e:
                logging.error(f"redis error: {e}")
                return
        return inner

    @_redis_avl
    def ping(self):
        return self.redis_client.ping()

    @_redis_avl
    def get_key(self, key: str):
        return self.redis_client.get(key)

    @_redis_avl
    def set_key(self, key: str, val: any):
        self.redis_client.set(key, val)

    @_redis_avl
    def get_key_json(self, key: str):
        return self.redis_client.json().get(key) 

    @_redis_avl
    def set_key_json(self, key: str, val: dict | list, encoderCls=JSONEncoder):
        val = json.loads(json.dumps(val, sort_keys=True, cls=encoderCls))
        self.redis_client.json().set(key, "$", val)

    @_pipelined
    def pset_keys_json(self, docs: list[RedisDocument]):
        """
        Desc: Set mutitple json keys using redis pipelining
        """
        for d in docs:
            self.pipe.json().set(d.key, "$", d.val)
