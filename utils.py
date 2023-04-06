##utils

def standardize_column(df):

    columns = df.columns

    new_columns = [elm.replace('.','_') for elm in columns]

    df.columns = new_columns

    return df
    
