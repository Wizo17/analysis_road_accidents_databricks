
def delete_technical_columns(df):
    """
    Delete technical columns from a DataFrame.
    
    Parameters:
    df (DataFrame): The input DataFrame from which to delete technical columns.
    
    Returns:
    DataFrame: A new DataFrame with technical columns removed.
    """
    technical_columns = ["source_file", "ingestion_timestamp", "ingestion_date"]
    return df.drop(*technical_columns)
