import pandas as pd
from pandas import *
import os
import numpy as np

os.chdir('X:/Users/Josh/Desktop/POKER PROJECT/StageG')

handnos = {}
k = 0
for i in range(1,15):
    dbhands = read_csv('hand_no_' + str(i) + '.csv', header = None).values.tolist()
    for j in dbhands:
        if j[1] in handnos:
            handnos[j[1]].append(i)
        else:
            handnos[j[1]] = []

handnos_dups = {}

for i in handnos:
    if len(handnos[i]) > 0:
        handnos_dups[i] = [handnos[i][0]]
        if len(handnos[i]) >= 2:
            handnos_dups[i].append(handnos[i][1])
        else:
            handnos_dups[i].append('')
        if len(handnos[i]) >= 3:
            print 'shouldnt be here'

df = DataFrame.from_dict(handnos_dups, orient = 'index')
df.columns = 'DB1','DB2'
df.index.name = 'Hand'
df.to_csv('x:/users/josh/desktop/poker project/StageH - recreating cash_cache/DUPLICATES.csv')
