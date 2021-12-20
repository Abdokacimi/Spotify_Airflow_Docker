import pandas as pd
import ETL_spotify_extract as extract


song_df = extract.extract_from_spotify()


def check_if_valid_data(df: pd.DataFrame) -> bool:
    # Check if dataframe is empty
    if df.empty:
        print("No songs downloaded. Finishing execution")
        return False

    # Primary Key Check
    if pd.Series(df['played_at']).is_unique:
        pass
    else:
        raise Exception("Primary Key check is violated")

    # Check for nulls
    if df.isnull().values.any():
        raise Exception("Null values found")

    return True


# Validate
def validate():
    if check_if_valid_data(song_df):
        print("Data valid, proceed to Load stage")
    else:
        print("Data not valid !!")
