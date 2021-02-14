'''
compare with pandas profling
'''
import pandas as pd 
from pandas_profiling import ProfileReport
import time

df = pd.read_csv('data/Meteorite_Landings.csv')
df.head()

start = time.time()
profile = ProfileReport(df, title="Meteorite_Landings_pandas")
profile.to_file("Meteorite_Landings_pandas.html")
print('processing ... report : ', time.time() - start, 's')
