from functools import wraps
from urllib.parse import urlencode
import pandas as pd

import pyarrow as pa

from .quivers import stash_bytes, get_bytes


def pyarrow_byte_cache(redis_uri, expires):
    """Cache decorator for use in caching pyarrow seralizable 
    objects. Decorated function must return something seralizable by 
    `pa.seralize`.

    Args:
        redis_uri (string): Redis URI to use, ex: redis://localhost/6379
        expires (int): number of seconds until cache expiration
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Generate the cache key from the function's arguments.
            key_parts = [func.__name__] + list(args) + [urlencode(kwargs)]
            key = ".".join(key_parts)
            res = get_bytes(redis_uri, key)

            if not res:
                value = func(*args, **kwargs)
                if not isinstance(value, pd.DataFrame):
                    raise TypeError("Function must return a dataframe.")
                _bytes = pa.serialize(value).to_buffer().to_pybytes()
                stash_bytes(redis_uri, _bytes, key, expires)
                print(f"Filling cache: {key}")
                return value
            else:
                return_object = pa.deserialize(res)
                print(f"Cache hit!")
                return return_object

        return wrapper

    return decorator 