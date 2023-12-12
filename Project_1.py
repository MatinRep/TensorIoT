pip install pandas pyarrow
import pandas as pd
import numpy as np

df = pd.read_csv('100k_a.csv')

# Records Count Check
df.head(10)
total = df.shape[0]
print("total:",total)
# Total number of Records = 3051733

print(df.head())

# Reloading and setting column name as per the Metadata:
file_path = '100k_a.csv'
columns_names = ['User_Id','Streaming_Id','Streamer_Username','Time_Start','Time_Stop']


df = pd.read_csv(file_path, names = columns_names)


print(df.head(10))

# General Check
distinct_id = df['User_Id'].drop_duplicates().head(10)
print(distinct_id)

# Item having the least rating:

least_rated_item = df.loc[df['Streaming_Id'].idxmin()]
print(least_rated_item)

#Output:
#User_Id                           33153
#Streaming_Id                33801525760
#Streamer_Username    dutchsinseofficial
#Time_Start                            1
#Time_Stop                             5
#Name: 1000526, dtype: object

# Item having the most rating:

most_rated_item = df.loc[df['Streaming_Id'].idxmax()]
print(most_rated_item)

#Output:
#User_Id                    14089
#Streaming_Id         34416415136
#Streamer_Username       alanzoka
#Time_Start                  6147
#Time_Stop                   6148
#Name: 418446, dtype: object

# Item having the longest reviews:

df['result'] = df['Time_Stop'] - df['Time_Start']  
df_diff = df.sort_values(by='result', ascending=False)
longest_Review =df_diff.head(7)
print(longest_Review)

#Output:
#Longest_Review  User_Id  Streaming_Id Streamer_Username  Time_Start  Time_Stop  
#2936758          96234   34204026640   beyondthesummit        3933       4030   
#1358248          44891   34103941584    gamesdonequick        2847       2939   
#1657631          54650   34103941584    gamesdonequick        2847       2939   
#2302318          75954   34399705520         fps_shaka        5955       6047   
#1010404          33445   34195934480           kaen_sg        3859       3949   
#1833902          60640   34102743088              uzra        2832       2922   
#1563774          51619   34102743088              uzra        2834       2922   


# Show a desired data frame operation (e.g., group by and calculate mean):

average_rating = df.groupby('Streaming_Id')['result'].mean()
print(average_rating.head(7))

#Output:
#Streaming_Id
#33801525760    4.0
#33802122720    8.0
#33802127264    1.0
#33802818320    7.0
#33802960400    2.0
#33805301168    1.0
#33807170560    2.0
#Name: result, dtype: float64

# Convert the whole file into Parquet file after transforming:
parquet_file = df.to_parquet(engine='pyarrow')

