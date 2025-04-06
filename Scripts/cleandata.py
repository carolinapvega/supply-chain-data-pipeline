import pandas as pd
import logging
import matplotlib.pyplot as plt
import seaborn as sns 
import os

#Create  Logging 
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler("supply-chain-data-pipeline/logs/cleaning_log.txt"),
        logging.StreamHandler()  # also prints to console
    ]
)



# Load Data

df=pd.read_csv('supply-chain-data-pipeline/Data/supply_chain_data.csv')

df_validation = df.copy()



# Firts Checks
pd.set_option('display.max_columns', None)
print(df.info())
initial_rows=df.shape[0]

# changing columns names
df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')
df_validation.columns = df_validation.columns.str.strip().str.lower().str.replace(' ', '_')
print(df.info())
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


#analising the data 
def generate_visual_reports(df, columns, output_dir):

    for col in columns:
        plt.figure(figsize=(12, 5))

        # Boxplot
        plt.subplot(1, 2, 1)
        sns.boxplot(data=df, y=col, color='skyblue')
        plt.title(f'Boxplot: {col}')

        # Histogram
        plt.subplot(1, 2, 2)
        sns.histplot(data=df, x=col, kde=True, bins=30, color='lightgreen')
        plt.title(f'Histogram: {col}')

        # Save figure
        plot_path = os.path.join(output_dir, f'{col}_box_hist.png')
        plt.tight_layout()
        plt.savefig(plot_path)
        plt.close()
        logging.info(f"📊 Visual report saved: {plot_path}")

generate_visual_reports(
    df,
    columns=['price', 'stock_levels', 'shipping_costs', 'defect_rates', 'manufacturing_costs'],     output_dir='supply-chain-data-pipeline/reports_outputs/plots_before_cleaning'

)


#Handling missing data

def missingdata(dataframe):
    missingdatapercentage = pd.DataFrame({
    'Columns': dataframe.columns,
    'Zeros': (dataframe == 0).mean() * 100,
    'NaN': dataframe.isna().mean() * 100,
    'Nulls': (dataframe.isnull() | (dataframe == '')).mean() * 100
    
})

    return missingdatapercentage

missingdata(df)
missingdata(df).to_csv('supply-chain-data-pipeline/reports_outputs/missingdatareport.csv')

# Drop rows with missing critical fields
df.dropna(subset=['sku', 'product_type', 'price', 'supplier_name', 'number_of_products_sold'], inplace=True)

after_drop_rows = df.shape[0]
rows_dropped = initial_rows - after_drop_rows
logging.info(f"Dropped {rows_dropped} rows with missing critical values.")

# Options for filling the missing data
# Fill in less critical fields with 0 or stignr
df['stock_levels'].fillna(0, inplace=True)
df['shipping_carriers'].fillna('UNKNOWN', inplace=True)

#Fill with previous rows value
#df['stock_levels'] = df['stock_levels'].ffill()

#fill with next values rows
#df['stock_levels'] = df['stock_levels'].bfill()

#fill with medium values rows

#median_value = df['stock_levels'].median()
#df['stock_levels'].fillna(median_value, inplace=True)
def compare_summary(before_df, after_df, columns, output_path):
    summary = []
    for col in columns:
        row = {
            'column': col,
            'mean_before': before_df[col].mean(),
            'mean_after': after_df[col].mean(),
            'std_before': before_df[col].std(),
            'std_after': after_df[col].std(),
            'missing_before': before_df[col].isna().sum(),
            'missing_after': after_df[col].isna().sum()
        }
        summary.append(row)

    summary_df = pd.DataFrame(summary)
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    summary_df.to_csv(output_path, index=False)
    logging.info(f"📊 Before/After summary saved to {output_path}")


compare_summary(
    before_df=df_validation,
    after_df=df,
    columns=[
        'price', 'stock_levels', 'shipping_costs', 
        'defect_rates', 'manufacturing_costs'
    ],
    output_path='supply-chain-data-pipeline/reports_outputs/cleaning_impact_summary.csv'
)



generate_visual_reports(
    df,
    columns=['price', 'stock_levels', 'shipping_costs', 'defect_rates', 'manufacturing_costs'],     output_dir='supply-chain-data-pipeline/reports_outputs/plots_after_cleaning'

)   

df.to_csv('supply-chain-data-pipeline/Data/cleaned_supply_chain_data.csv', index=False)
logging.info("Cleaned data saved to: cleaned_supply_chain_data.csv")
logging.info(f" Max values {df['number_of_products_sold'].max()} .")
logging.info(f" Min values {df['number_of_products_sold'].min()} .")
logging.info("✅ Cleaning complete.")
logging.info(f"Final row count: {df.shape[0]}")
logging.info(f"Columns in final dataset: {list(df.columns)}")
