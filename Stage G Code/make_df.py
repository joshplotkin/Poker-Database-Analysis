import pandas as pd
from pandas import *
import os

os.chdir('X:\Users\Josh\Desktop\POKER PROJECT\StageG')

class Chunk:
    def __init__(self):
        self.fields = []
        self.length = 0

    def add(self, field):
        self.fields.append(str(field))
        self.length += 1

    def pop(self):
        self.length -= 1
        if self.length < 0: # list empty
            return False
        return self.fields[self.length]

    def get_len(self):
        return self.length

    def get_list(self):
        return self.fields

class Holder:
    def __init__(self,name,ch = None):
        self.on = False
        self.field = name
        self.chunk = ch

def add_field(field,chunk):
    obj = Holder(field,chunk)
    obj.on = True
    fields.append(obj)

def process_fields(fields):
    for f in fields: # f has chunk, on, field
        ch = f.chunk
        curr = df[f.chunk]

        counter = 0
        for c in curr:
            if c.field == f.field:
                df.loc[counter,f.chunk] = f
                return True
            counter += 1

    return False


def print_df(df):
    newdf = {}
    for i in df.index:
        newdf[i] = []
        for j in df.columns:
            currobj = df.loc[i,j]
            if currobj.field == None:
                pass
            else:
                if currobj.on == True:
                    onmsg = '***'
                else:
                    onmsg = ''
                text = currobj.field + '(' + onmsg + ')'
                newdf[i].append(text)

    return DataFrame.from_dict(newdf, orient = 'index')

def return_summ(db):
#     summ = read_csv('db' + str(db) + '_SUMMARY.csv')
    summ = read_csv('db2_SUMMARY.csv')
    print summ.head()

    def replace_nan(x):
        if isinstance(x, float):
            return ''
        else:
            return x

    for col in ('action_p', 'action_f', 'action_t', 'action_r'):
        summ[col] = summ[col].apply(replace_nan)

    return summ

def make_df(field_df,db,name,final_df):

    execute = []
    for col in field_df.columns: # chunk
        for row in field_df.index:
            if field_df.loc[row,col].on == True:
                curr = field_df.iloc[row,col].field
                execute.append(col,row)

    dfex = DataFrame(execute)
    dfex.columns = 'Chunk','Field'
    print dfex

    return
    chunks_to_do = set(dfex.Chunk.values)

    for ch in chunks_to_do:
        dfch = read_csv('db' + ch + '_SUMMARY.csv', index_col = ('id_hand','player_name'))
        final_df = final_df.join(dfch[[chunks[ch].get_list()]], how = 'left')

    newpath = 'X:/Users/Josh/Desktop/POKER PROJECT/StageG/' + name
    if not os.path.exists(newpath): os.makedirs(newpath)
    os.chdir(newpath)

    final_df.to_csv(name + '_' + db + ',csv')


fields = []

add_field('flg_vpip',1)
add_field('cnt_p_raise',1)
add_field('amt_before',9)
add_field('amt_p_raise_facing',10)
add_field('amt_blind',9)
add_field('flg_p_open_opp',9)
add_field('cnt_p_face_limpers',1) # > 0
add_field('flg_p_3bet_opp',4)
add_field('flg_p_4bet_opp',4)

df = read_csv('X:/Users/Josh/Desktop/POKER PROJECT/StageG/chunks.csv')
process_fields(fields)
print print_df(df).head()

name = 'VPIP_PFR'
for db in range(1,2):
    make_df(df,db,name,return_summ(db))
