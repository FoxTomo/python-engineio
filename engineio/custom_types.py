import pickle

from typing import Hashable

import redis


class RedisDict(dict):
    def __init__(self, redis_host, redis_port, namespace, ttl=None, seq=None, **kwargs):
        self.redis_cli = getattr(self.__class__, "redis_cli", None)
        if not self.redis_cli:
            self.redis_cli = redis.Redis(redis_host, redis_port)
            setattr(self.__class__, "redis_cli", self.redis_cli)
        self.namespace = namespace
        self.ttl = ttl
        if seq:
            if isinstance(seq, dict):
                seq = seq.items()
            for key, value in seq:
                self.__setitem__(key, value)
        for key, value in kwargs.items():
            self.__setitem__(key, value)
        super().__init__(seq or {}, **kwargs)

    def __set_to_redis(self, k, v):
        self.redis_cli.set(f"{self.namespace}_{str(k)}", pickle.dumps(v), ex=self.ttl)

    def __get_from_redis(self, k):
        result = self.redis_cli.get(f"{self.namespace}_{str(k)}")

        if result:
            result = pickle.loads(result)

        return result

    def __key_in_redis(self, k):
        return self.redis_cli.exists(f"{self.namespace}_{str(k)}")

    def __del_key_from_redis(self, k):
        self.redis_cli.delete(f"{self.namespace}_{str(k)}")

    def __return_all_keys_of_namespace(self):
        return self.redis_cli.keys(f"{self.namespace}_*")

    def __delitem__(self, key):
        self.__check_for_hashable(key)
        self.__del_key_from_redis(key)

    def __getitem__(self, item):
        self.__check_for_hashable(item)
        if not self.__key_in_redis(item):
            raise KeyError(item)
        result = self.__get_from_redis(item)
        return result

    def __len__(self):
        return len(self.__return_all_keys_of_namespace())

    @staticmethod
    def __check_for_hashable(key):
        if not isinstance(key, Hashable):
            raise TypeError(f"Key {key} is not hashable.")

    def __setitem__(self, key, value):
        self.__check_for_hashable(key)
        self.__set_to_redis(key, value)

    def get(self, k, default=None):
        result = self.__get_from_redis(k)
        if result is None:
            return default
        return result

    def update(self, e=None, **f):
        if e:
            if isinstance(e, dict):
                e = e.items()
            for key, value in e:
                self.__setitem__(key, value)
        for key, value in f.items():
            self.__setitem__(key, value)

    def __handle_key(self, key, clear_namespace=False):
        if clear_namespace:
            return key.replace(f"{self.namespace}_", "")
        return key

    def __get_keys(self, clear_namespace=False):
        result = self.__return_all_keys_of_namespace()
        return [self.__handle_key(key.decode(), clear_namespace) for key in result]

    def __get_values(self, with_keys=False, clear_namespace=False):
        keys = self.keys()
        values = []
        exception_class = type("ValueNotExists", (Exception, ), {})()
        for key in keys:
            value = self.get(key, exception_class)
            if value is exception_class:
                continue
            values.append(
                value
                if not with_keys
                else (self.__handle_key(key, clear_namespace), value)
            )
        return values

    def values(self):
        return self.__get_values()

    def keys(self):
        return self.__get_keys(True)

    def items(self):
        return self.__get_values(True, True)

    def pop(self, key):
        self.__check_for_hashable(key)
        result = self.__getitem__(key)
        self.__delitem__(key)
        return result

    @classmethod
    def fromkeys(cls, *args, **kwargs):
        raise NotImplementedError

    def copy(self):
        raise NotImplementedError

    def popitem(self):
        raise NotImplementedError

    def clear(self):
        keys = self.__get_keys(True)
        for key in keys:
            self.__delitem__(key)

    def __contains__(self, item):
        self.__check_for_hashable(item)
        return self.__key_in_redis(item)
