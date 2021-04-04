# Quive[rs]

Extremely lightweight data caching decorator for compressing pyarrow objects and serializing them to redis, written as a rust extension. Binary data is compressed using snappy, then decompressed on retrieval from cache.

**Why?** To save time in interactive contexts, like a notebook. Decorate your function that pulls the data and set the cache expiry to something like 3 hours. If you repeatedly call the function (even after the kernel dies!) the results will be returned from cache.
Profit.

## Example

```python
from quivers import pyarrow_byte_cache

@pyarrow_byte_cache("redis://localhost:6379", expires=3*60*60)
def make_data(size: int = 100) -> pd.DataFrame:
    df = pd.DataFrame(columns=["First", "Last", "Gender", "Birthdate"])
    df["First"] = random_names("first_names", size)
    df["Last"] = random_names("last_names", size)
    df["Gender"] = random_genders(size)
    df["Birthdate"] = random_dates(
        start=pd.to_datetime("1940-01-01"), end=pd.to_datetime("2008-01-01"), size=size
    )
    return df
```

Subsequent calls to this function will return the compressed data from redis instead of recomputing the value, after 3 hours the data will be recomputed again.

## Develop

This project builds python wheels using `maturin` and requires `poetry`.

To build, first install poetry, then run:

```sh
poetry run maturin develop
```
