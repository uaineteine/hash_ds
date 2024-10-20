print("[hash_ds.py] importing libraries...")
import pandas as pd
import hmac
import hashlib
import swifter
from multiprocessing import cpu_count

print("[hash_ds.py] defining functions...")
def hmac_and_truncate_value(value, key, length=24):
    hashed_value = hmac.new(key.encode(), str(value).encode(), hashlib.sha256).hexdigest()
    return hashed_value[:length]

def hash_and_truncate_hmac_swifter(df, column, key, length=24):
    print(f"Using up to {cpu_count()} threads")
    new_col_name = column + "_hash"
    df[new_col_name] = df[column].swifter.set_npartitions(cpu_count()).apply(lambda x: hmac_and_truncate_value(x, key, length))
    return df

def read_file(filepath):
    format = filepath.split('.')[-1]
    if format == "csv":
        return pd.read_csv(filepath)
    elif format in ("xlsx", "xls"):
        return pd.read_excel(filepath)
    elif format == "sas7bdat":
        return pd.read_sas(filepath)
    elif format == "psv":
        return pd.read_csv(filepath, delimiter='|')
    else:
        raise ValueError("Unsupported file extension")

def read_and_clean_file(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        content = file.read()
    
    # Remove additional empty lines and trailing whitespace
    cleaned_content = "\n".join([line.rstrip() for line in content.splitlines() if line.strip()])
    
    return cleaned_content

print("[hash_ds.py] reading hash_key from file")
key_path = "key.txt"
key = read_and_clean_file(key_path)

print("[hash_ds.py] reading template from file")
template = read_file("testdata/template.csv")
print(template)

print("[hash_ds.py] performing hashing...")
for i, fname in enumerate(template["Input_file_path"]):
    mname = template["Meta_file_path"][i] #metaname, at ith row
    meta = read_file(mname)

df = read_file('testdata/data2.csv')
hashed_df = hash_and_truncate_hmac_swifter(df, 'usernames', key)
print(hashed_df)

print("hashing second set")
df = read_file('testdata/data1.psv')
hashed_df = hash_and_truncate_hmac_swifter(df, 'usernames', key)
print(hashed_df)


