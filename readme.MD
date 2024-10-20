## HASH_DS

[![Test hash_ds.py](https://github.com/uaineteine/hash_ds/actions/workflows/test_hash_ds.yml/badge.svg)](https://github.com/uaineteine/hash_ds/actions/workflows/test_hash_ds.yml)

A tool to hash dataframes on a list of variables and a key. The python script it multi-threaded to optimise to the number of user cores that are available.

## Example execute

One can select the truncation limit for this output

```ps
python -m hash_ds testdata/data1.psv usernames,Header2 secret_key 10
```