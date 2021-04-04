from functools import wraps
from urllib.parse import urlencode

import numpy as np
import pandas as pd
import pyarrow as pa
from faker.providers.person.en import Provider
from redis import StrictRedis
from quivers import get_bytes, stash_bytes, pyarrow_byte_cache


def random_names(name_type, size):
    """
    Generate n-length ndarray of person names.
    name_type: a string, either first_names or last_names
    """
    names = getattr(Provider, name_type)
    return np.random.choice(names, size=size)


def random_genders(size, p=None):
    """Generate n-length ndarray of genders."""
    if not p:
        # default probabilities
        p = (0.49, 0.49, 0.01, 0.01)
    gender = ("M", "F", "O", "")
    return np.random.choice(gender, size=size, p=p)


def random_dates(start, end, size):
    """
    Generate random dates within range between start and end.
    Adapted from: https://stackoverflow.com/a/50668285
    """
    # Unix timestamp is in nanoseconds by default, so divide it by
    # 24*60*60*10**9 to convert to days.
    divide_by = 24 * 60 * 60 * 10 ** 9
    start_u = start.value // divide_by
    end_u = end.value // divide_by
    return pd.to_datetime(np.random.randint(start_u, end_u, size), unit="D")


@pyarrow_byte_cache("redis://localhost:6379", expires=5)
def make_data(size: int = 100) -> pd.DataFrame:
    df = pd.DataFrame(columns=["First", "Last", "Gender", "Birthdate"])
    df["First"] = random_names("first_names", size)
    df["Last"] = random_names("last_names", size)
    df["Gender"] = random_genders(size)
    df["Birthdate"] = random_dates(
        start=pd.to_datetime("1940-01-01"), end=pd.to_datetime("2008-01-01"), size=size
    )
    return df


# for size in np.random.randint(100000, 100050, 200):
#     df = make_data(size=size)
#     print(df.shape)

size = 234973
df = make_data(size=size)
print(df.info())

# import cProfile
# cProfile.run('make_data(size=39999)')
# cProfile.run('make_data(size=40001)')


r = StrictRedis()
print("REDIS mem usage:", r.memory_usage(f"make_data.size={size}"))
