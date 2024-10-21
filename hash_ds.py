from libs import *

print("[hash_ds::hash_ds.py] defining functions...")
def hmac_and_truncate_value(value, key, length=24):
    hashed_value = hmac.new(key.encode(), str(value).encode(), hashlib.sha256).hexdigest()
    return hashed_value[:length]

def hash_and_truncate_hmac_swifter(df, columns, key, length=24):
    n_tds = cpu_count()
    print(f"Using up to {n_tds} threads")
    for column in columns:
        new_col_name = column + "_hash"
        df[new_col_name] = df[column].swifter.set_npartitions(n_tds).apply(lambda x: hmac_and_truncate_value(x, key, length))
    return df

def main():
    print("[hash_ds::hash_ds.py] reading arguments...")
    parser = argparse.ArgumentParser(description="Hash specified columns of a file")
    parser.add_argument('filepath', type=str, help='Path to the file to hash')
    parser.add_argument('columns', type=str, help='Comma-separated list of columns to hash')
    parser.add_argument('key', type=str, help='Key for hashing')
    parser.add_argument('length', type=int, help='Length of hash to truncate to')
    parser.add_argument('output_loc', type=str, help='Location to save the output file')
    
    args = parser.parse_args()
    filepath = args.filepath
    columns = args.columns.split(',')
    key = args.key
    length = args.length
    output_loc = args.output_loc

    print("[hash_ds::hash_ds.py] reading data")
    df = read_file(filepath)
    print("[hash_ds::hash_ds.py] hashing")
    hashed_df = hash_and_truncate_hmac_swifter(df, columns, key, length=length)
    print("[hash_ds::hash_ds.py] saving data")
    hashed_df.to_parquet(output_loc, index=False)
    #print(hashed_df)

if __name__ == "__main__":
    main()
