# Trent Adams
# 2024-02-12 20:10:41 UTC-5

# This project makes use of the following packages: pandas, numpy, asyncio, aiofiles. The purpose for the project is to 
# generate a large amount of financial data quickly and save it to a CSV file. I've chosen financial data, however,
# the project can be used for any kind of data. The data is generated using the generate_financial_data function.

# There are 6 csv files included in this project. In keeping with the track of the course the files contain the following
# numbers of rows: 100, 1000, 10000, 200000, 400000 and 600000. The files are named financial_data_100.csv,
# financial_data_1000.csv, financial_data_10000.csv, financial_data_200000.csv, financial_data_400000.csv and
# financial_data_600000.csv.

import pandas as pd
import numpy as np
import asyncio
import aiofiles

# Define your data generation logic here
# Example data for demonstration purposes
def generate_financial_data(num_rows=600000):
    market_segments = ['Technology', 'Healthcare', 'Finance', 'Manufacturing', 'Education']
    countries = ['USA', 'Canada', 'Germany', 'France', 'UK', 'Japan', 'China', 'India', 'Brazil', 'Australia']
    np.random.seed(42)
    sales = np.random.uniform(1000, 50000, num_rows)
    profit = sales * np.random.uniform(0.1, 0.5, num_rows)
    market_segment_data = np.random.choice(market_segments, num_rows)
    country_data = np.random.choice(countries, num_rows)

    financial_data = pd.DataFrame({
        'Market Segment': market_segment_data,
        'Country/Region': country_data,
        'Sales': np.around(sales, 2),
        'Profit': np.around(profit, 2)
    })
    
    return financial_data

# Asynchronously write CSV content to a file
async def write_csv_file(file_name, content):
    async with aiofiles.open(file_name, 'w') as file:
        await file.write(content)

# Main async function to generate data and save it to a CSV file
async def generate_and_save_csv():
    financial_data = generate_financial_data()  # Generate the data
    csv_content = financial_data.to_csv(index=False, lineterminator='\n')  # Convert to CSV string
    
    # Correcting potential double carriage returns if any (CR LF issue)
    corrected_csv_content = csv_content.replace('\r\n', '\n')
    
    # Write the corrected CSV content to a file
    await write_csv_file('financial_data_600000.csv', corrected_csv_content)

# Execute the async function
asyncio.run(generate_and_save_csv())

print("CSV file has been generated and saved successfully.")
