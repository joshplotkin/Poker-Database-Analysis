import pandas as pd
from pandas import *

def find_field(field):
    table = read_csv('X:/Users/Josh/Desktop/POKER PROJECT/ALL TABLES.csv')

    for col in table.columns:
        if field in table[col].values.tolist():
            return col

    print field + ' problem'

df = read_csv('All Dependencies.csv', index_col = 'Unnamed: 0')

# counter, variable, field, number

flatlist = {}

maincols = []
for c in df.columns:
    if c[-2:] != '_n':
        maincols.append(c)

# maxx = 0
counter = 0
for c in range(len(df.columns)):
    if df.columns[c] not in maincols:
        pass
    else:
        for r in range(len(df[df.columns[c]])):
            if type(df.iloc[r,c]) is str:
                flatlist[counter] = []
                flatlist[counter].append(df.columns[c]) # append variable
                flatlist[counter].append(df.iloc[r,c]) # append field
                flatlist[counter].append(df.iloc[r,c+1]) # table number

                counter += 1


flatdf = DataFrame.from_dict(flatlist, orient = 'index')
flatdf.columns = 'Variable','Field','Table'

flatdf['Dependencies'] = 1

print flatdf.head()
print len(flatdf)

grouped = flatdf[['Variable','Dependencies']].groupby('Variable').sum()

print grouped

del flatdf['Dependencies']
flatdf = flatdf.merge(grouped, left_on = 'Variable', right_index = True, how = 'left')

print flatdf.columns

flatdf = flatdf[['Variable','Field','Table','Dependencies']]
flatdf.index.name = 'i'
flatdf.to_csv('X:/Users/Josh/Desktop/POKER PROJECT/StageH - recreating cash_cache/Flat List.csv')
