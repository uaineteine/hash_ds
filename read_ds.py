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
    