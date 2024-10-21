from libs import *

print("[hash_ds.py] defining functions...")
def hmac_and_truncate_value(value, key, length=24):
    hashed_value = hmac.new(key.encode(), str(value).encode(), hashlib.sha256).hexdigest()
    return hashed_value[:length]

def hash_and_truncate_hmac_swifter(df, columns, key, length=24):
    print(f"Using up to {cpu_count()} threads")
    for column in columns:
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
    elif format == "parquet":
        return pd.read_parquet(filepath)
    else:
        raise ValueError("Unsupported file extension")

def main():
    parser = argparse.ArgumentParser(description="Hash specified columns of a file")
    parser.add_argument('filepath', type=str, help='Path to the file to hash')
    parser.add_argument('columns', type=str, help='Comma-separated list of columns to hash')
    parser.add_argument('key', type=str, help='Key for hashing')
    parser.add_argument('length', type=int, help='Length of hash to truncate to')
    
    args = parser.parse_args()
    filepath = args.filepath
    columns = args.columns.split(',')
    key = args.key
    length = args.length

    df = read_file(filepath)
    hashed_df = hash_and_truncate_hmac_swifter(df, columns, key, length=length)
    print(hashed_df)

if __name__ == "__main__":
    main()

# print("[hash_ds.py] reading template from file")
# template = read_file("testdata/template.csv")
# print(template)

# print("[hash_ds.py] performing hashing...")
# for i, fname in enumerate(template["Input_file_path"]):
#     mname = template["Meta_file_path"][i] #metaname, at ith row
#     meta = read_file(mname)

# df = read_file('testdata/data2.csv')
# hashed_df = hash_and_truncate_hmac_swifter(df, 'usernames', key)
# print(hashed_df)

# print("hashing second set")
# df = read_file('testdata/data1.psv')
# hashed_df = hash_and_truncate_hmac_swifter(df, 'usernames', key)
# print(hashed_df)


