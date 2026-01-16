import sys
import pandas as pd

# Take a file that contains temporal data and process data only from december
# https://www.youtube.com/watch?v=lP8xXebHmuE&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=3 T=33.21

print('arguments:', sys.argv)

month = int(sys.argv[1])

df = pd.DataFrame({"day": [1, 2, 3], "num_passengers": [4, 5, 6]})
df['month'] = month
print(df.head())

df.to_parquet(f"data_for_month_{month}.parquet")

print(f'Processing data for month: {month}')