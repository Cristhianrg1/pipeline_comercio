import pandas as pd


def parse_dates(df, date_columns):
    for col in date_columns:
        temp_col1 = pd.to_datetime(df[col], format="%Y-%m-%d", errors="coerce")
        temp_col2 = pd.to_datetime(df[col], format="%Y/%m/%d", errors="coerce")
        df[col] = temp_col1.combine_first(temp_col2)
    return df


def format_numeric_values(df, numeric_columns):
    for col in numeric_columns:
        df[col] = df[col].str.replace(",", ".").astype(float)
    return df
