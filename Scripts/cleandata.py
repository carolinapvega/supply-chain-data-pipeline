import pandas as pd


# Load Data

df=pd.read_csv('/Users/carolinavega/Documents/Python/PraticandoGithub/supply-chain-data-pipeline/Data/supply_chain_data.csv')


# Firts Checks
pd.set_option('display.max_columns', None)
df.head()
df.columns
df.dtypes

# changing columns names
df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')

# 1. adjusting Strings
def adjustingstrings(database, column):
    
    database[column] = database[column].str.upper()  
    database[column] = database[column].str.strip() 
    
    return database[column]


adjustingstrings(df,'product_type')
adjustingstrings(df,'customer_demographics')
adjustingstrings(df,'shipping_carriers')
adjustingstrings(df,'supplier_name')
adjustingstrings(df,'location')
adjustingstrings(df,'transportation_modes')
adjustingstrings(df,'routes')

#Cjecking Unique Values for string columns
df['product_type'].unique()
df['customer_demographics'].unique()
df['shipping_carriers'].unique()
df['supplier_name'].unique()
df['location'].unique()
df['transportation_modes'].unique()
df['routes'].unique()


def missingdata(dataframe):
    missingdatapercentage = pd.DataFrame({
    'Columns': dataframe.columns,
    'Zeros': (dataframe == 0).mean() * 100,
    'NaN': dataframe.isna().mean() * 100,
    'Nulls': (dataframe.isnull() | (dataframe == '')).mean() * 100
    
})

    return missingdatapercentage

missingdata(df)