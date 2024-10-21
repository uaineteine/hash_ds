from libs import *

print("[hash_ds::.py] defining functions...")
def hmac_and_truncate_value(value, key, length=24):
    hashed_value = hmac.new(key.encode(), str(value).encode(), hashlib.sha256).hexdigest()
    return hashed_value[:length]

def hash_and_truncate_hmac_swifter(df, columns, key, length=24):
    print(f"Using up to {cpu_count()} threads")
    for column in columns:
        new_col_name = column + "_hash"
        df[new_col_name] = df[column].swifter.set_npartitions(cpu_count()).apply(lambda x: hmac_and_truncate_value(x, key, length))
    return df

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
