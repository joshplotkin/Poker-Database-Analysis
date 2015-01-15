import pandas as pd
import numpy as np
from pandas import *
import os
import psycopg2
import sys
import random

pd.set_option('max_rows',4000)
pd.set_option('max_columns',1000)
pandas.set_option('precision',3)
os.chdir('C:\Users\JP\Desktop\SCHOOL\Spring2014\EDAV\Poker data\Poker Project\Stage F - BTN v BB\cash_hand_player_statistics')

# database 4

df = read_csv('4/16_batch01_4.csv', index_col = ('id_hand','id_player'))
del df['Unnamed: 0']

df = df[df.id_gametype == 1]
df = df[np.logical_or(np.logical_or(df.id_limit == 1,df.id_limit == 5),df.id_limit == 6)]
# limit 6 is 100NL
keep_index = df.index.copy()

df = df[['id_limit','position','id_holecard']]

append = read_csv('4/16_batch02_4.csv', index_col = ('id_hand','id_player'))
print append.head()

# print df.head(25)