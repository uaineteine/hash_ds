import pandas as pd
import hmac
import hashlib
import swifter

def hmac_and_truncate_value(value, key, length=24):
    hashed_value = hmac.new(key.encode(), str(value).encode(), hashlib.sha256).hexdigest()
    return hashed_value[:length]

def hash_and_truncate_hmac_swifter(df, column, key, length=24):
    new_col_name = column + "_hash"
    df[new_col_name] = df[column].swifter.apply(lambda x: hmac_and_truncate_value(x, key, length))
    return df

# Example usage
data = {'usernames': ['user1', 'user2', 'user3']}
df = pd.DataFrame(data)
hashed_df = hash_and_truncate_hmac_swifter(df, 'usernames', 'my_secret_key')
print(hashed_df)
