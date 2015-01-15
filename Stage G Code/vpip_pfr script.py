import pandas as pd
from pandas import *
import math
import numpy as np
import os

def update_main_df(db, df, name, start):
    ## make big VPIP df
    if 'Unnamed: 0' in df.columns:
        del df['Unnamed: 0']

    for i in df.columns:
    #     print type(df[i].values[0])
        if type(df[i].values[0]) is bool:
            df[i] = df[i].apply(lambda x: x.astype(int))

        df.cnt_p_face_limpers = df.cnt_p_face_limpers.apply(lambda x: 1 if x > 0 else 0)

    df['cnt_vpip'] =  df.flg_vpip.apply(lambda x: 1 if x == True else 0)
    df['cnt_pfr'] = df.cnt_p_raise
    df['cnt_vpip_opp'] = df.action_p.apply(lambda x: 1 if type(x) is str else 0)

    df['part1'] = df.amt_before > 2 # started the hand with money
    df['part2'] = (df.amt_p_raise_facing < df.amt_before - df.amt_blind) # not facing all-in
    df['part3'] = df.flg_p_open_opp + df.cnt_p_face_limpers + df.flg_p_3bet_opp + df.flg_p_4bet_opp # could raise
    df['part4'] = df.action_p.apply(lambda x: 1 if type(x) is str else 0)

    df.part1 = df.part1.apply(lambda x: x.astype(int))
    df.part2 = df.part2.apply(lambda x: x.astype(int))
    df.part3 = df.part3.apply(lambda x: 1 if x > 0 else 0)

    df['cnt_pfr_opp'] = (df.part1 + df.part2 + df.part3 + df.part4).apply(lambda x: 1 if x == 4 else 0)
    df = df[['player_name','amt_expected_won','cnt_vpip','cnt_vpip_opp','cnt_pfr','cnt_pfr_opp']]
    df['hands'] = 1
    df = df.groupby('player_name').sum()

    newpath = 'X:/Users/Josh/Desktop/POKER PROJECT/StageG/df_with_fields/' + name
    if not os.path.exists(newpath): os.makedirs(newpath)
    os.chdir(newpath)

    if db == start:
        df.to_csv('VPIP_PFR_ALL.csv')
    else:
        df_all = read_csv('VPIP_PFR_ALL.csv', index_col = 'player_name')
        if 'Unnamed: 0' in df.columns:
            del df_all['Unnamed: 0']
        df_all = df_all.append(df)
        df_all.to_csv('VPIP_PFR_ALL.csv')

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

def make_field_df():
    tables= {'cash_hand_histories': ['id_hand','history'], 'cash_hand_player_combinations': ['id_hand','id_gametype','id_player','flg_f_highcard','flg_f_1pair','flg_f_2pair','flg_f_threeoak','flg_f_straight','flg_f_flush','flg_f_fullhouse','flg_f_fouroak','flg_f_strflush','id_f_hand_strength','id_f_kicker_strength','val_f_hole_cards_used','flg_f_gutshot_draw','flg_f_straight_draw','flg_f_2gutshot_draw','flg_f_flush_draw','flg_f_wrap_draw','val_f_wrap_outs','flg_f_bflush_draw','flg_f_bstraight_draw','id_f_flush_draw_strength','val_f_straight_draw_nut_outs','val_f_straight_draw_nonnut_outs','flg_f_low_hand','val_f_low_hand_strength','flg_f_nut_flush_blocker','flg_f_low_draw','val_f_low_draw_strength','flg_t_highcard','flg_t_1pair','flg_t_2pair','flg_t_threeoak','flg_t_straight','flg_t_flush','flg_t_fullhouse','flg_t_fouroak','flg_t_strflush','id_t_hand_strength','id_t_kicker_strength','val_t_hole_cards_used','flg_t_gutshot_draw','flg_t_straight_draw','flg_t_2gutshot_draw','flg_t_flush_draw','flg_t_wrap_draw','val_t_wrap_outs','id_t_flush_draw_strength','val_t_straight_draw_nut_outs','val_t_straight_draw_nonnut_outs','flg_t_low_hand','val_t_low_hand_strength','flg_t_nut_flush_blocker','flg_t_low_draw','val_t_low_draw_strength','flg_r_highcard','flg_r_1pair','flg_r_2pair','flg_r_threeoak','flg_r_straight','flg_r_flush','flg_r_fullhouse','flg_r_fouroak','flg_r_strflush','id_r_hand_strength','id_r_kicker_strength','val_r_hole_cards_used','flg_r_low_hand','val_r_low_hand_strength','flg_r_nut_flush_blocker'], 'cash_hand_summary': ['id_hand','id_gametype','id_site','id_limit','id_table','hand_no','date_played','date_imported','cnt_players','cnt_players_lookup_position','cnt_players_f','cnt_players_t','cnt_players_r','amt_pot','amt_rake','amt_mgr','amt_pot_p','amt_pot_f','amt_pot_t','amt_pot_r','str_actors_p','str_actors_f','str_actors_t','str_actors_r','str_aggressors_p','str_aggressors_f','str_aggressors_t','str_aggressors_r','id_win_hand','id_win_hand_lo','id_winner','id_winner_lo','button','card_1','card_2','card_3','card_4','card_5','flg_note','flg_tag','flg_autonote'], 'cash_limit': ['id_limit','limit_name','id_gametype','limit_currency','amt_sb','amt_bb','flg_nlpl','flg_nl','flg_pl','flg_lo','flg_fr','flg_sh','flg_hu','id_filter','valid'], 'lookup_actions': ['id_action','action'], 'lookup_hand_groups': ['id_group','group_name'], 'lookup_hand_ranks': ['id_hand_rank','id_group','group_name','group_details'], 'lookup_hole_cards': ['id_holecard','id_gametype','hole_cards','enum_pair_type','val_pair_1','val_pair_2','flg_h_suited','flg_h_connector','flg_h_1_gap','flg_h_2_gap','flg_o_connector','flg_o_connector_2_str','flg_o_connector_2_gap','flg_o_connector_3'], 'lookup_positions': ['cnt_players','position','absolute_position','description','flg_sb','flg_bb','flg_ep','flg_mp','flg_co','flg_btn'], 'lookup_sites': ['id_site','site_abbrev','site_name'], 'player': ['id_player','id_site','player_name','player_name_search','id_player_alias','flg_note','flg_tag'], 'Summary': ['id_hand','player_name','amt_bb'], 1: ['id_hand','player_name','id_player','id_limit','id_gametype','position','id_player_real','id_holecard','id_session','date_played','cnt_players','cnt_players_lookup_position','cnt_p_raise','flg_p_first_raise','cnt_p_call','flg_p_limp','flg_p_fold','flg_p_ccall','cnt_p_face_limpers','flg_vpip','flg_f_bet','cnt_f_raise','flg_f_first_raise','limit_name'], 2: ['id_hand','player_name','cnt_f_call','flg_f_check','flg_f_check_raise','flg_f_fold','flg_f_saw','flg_t_bet','cnt_t_raise','flg_t_first_raise','cnt_t_call','flg_t_check','flg_t_check_raise','flg_t_fold','flg_t_saw','flg_r_bet','cnt_r_raise','flg_r_first_raise','cnt_r_call','flg_r_check','flg_r_check_raise','flg_r_fold'], 3: ['id_hand','player_name','flg_r_saw','enum_allin','enum_face_allin','enum_face_allin_action','flg_blind_s','flg_blind_b','flg_blind_ds','flg_blind_db','flg_sb_steal_fold','flg_bb_steal_fold','flg_blind_def_opp','flg_steal_att','flg_steal_opp','flg_blind_k','flg_showdown','flg_won_hand','amt_won','amt_expected_won','val_equity','amt_r_raise_made','hand_no','card_1','card_2','card_3','card_4','card_5'], 4: ['id_hand','player_name','id_final_hand','id_final_hand_lo','flg_showed','enum_folded','flg_p_face_raise','flg_p_3bet','flg_p_3bet_opp','flg_p_3bet_def_opp','enum_p_3bet_action','flg_p_4bet','flg_p_4bet_opp','flg_p_4bet_def_opp','enum_p_4bet_action','flg_p_squeeze','flg_p_squeeze_opp','flg_p_squeeze_def_opp','enum_p_squeeze_action','flg_f_face_raise','flg_f_3bet'], 5: ['id_hand','player_name','flg_f_3bet_opp','flg_f_3bet_def_opp','enum_f_3bet_action','flg_f_4bet','flg_f_4bet_opp','flg_f_4bet_def_opp','enum_f_4bet_action','flg_f_cbet','flg_f_cbet_opp','flg_f_cbet_def_opp','enum_f_cbet_action','flg_t_face_raise','flg_t_3bet','flg_t_3bet_opp','flg_t_3bet_def_opp','enum_t_3bet_action','flg_t_4bet','flg_t_4bet_opp','flg_t_4bet_def_opp','enum_t_4bet_action'], 6: ['id_hand','player_name','flg_t_cbet','flg_t_cbet_opp','amt_t_raise_made','val_t_raise_made_pct','amt_t_raise_made_2','val_t_raise_made_2_pct','amt_r_bet_facing','val_r_bet_facing_pct','val_r_bet_aggressor_pos','amt_r_bet_made','val_r_bet_made_pct','amt_r_raise_facing','val_r_raise_facing_pct','amt_r_2bet_facing','val_r_2bet_facing_pct','amt_r_3bet_facing','val_r_3bet_facing_pct','amt_r_4bet_facing','val_r_4bet_facing_pct','val_r_raise_aggressor_pos'], 7: ['id_hand','player_name','flg_t_cbet_def_opp','enum_t_cbet_action','flg_t_float','flg_t_float_opp','flg_t_float_def_opp','enum_t_float_action','flg_t_donk','flg_t_donk_opp','flg_t_donk_def_opp','enum_t_donk_action','flg_r_face_raise','flg_r_3bet','flg_r_3bet_opp','flg_r_3bet_def_opp','enum_r_3bet_action','flg_r_4bet','flg_r_4bet_opp','flg_r_4bet_def_opp','enum_r_4bet_action'], 8: ['id_hand','player_name','flg_r_cbet','flg_r_cbet_opp','flg_r_cbet_def_opp','enum_r_cbet_action','flg_r_float','flg_r_float_opp','flg_r_float_def_opp','enum_r_float_action','flg_r_donk','flg_r_donk_opp','flg_r_donk_def_opp','enum_r_donk_action','val_curr_conv','seat','holecard_1','holecard_2','holecard_3','holecard_4','id_suits','flg_hero'], 9: ['id_hand','player_name','amt_before','amt_blind','amt_ante','amt_bet_p','amt_bet_f','amt_bet_t','amt_bet_r','amt_bet_ttl','id_action_p','id_action_f','id_action_t','id_action_r','flg_p_open','flg_p_open_opp','flg_f_first','flg_f_open','flg_f_open_opp','flg_f_has_position','flg_t_first','val_r_raise_made_pct','action_p','action_f','action_t','action_r'], 10: ['id_hand','player_name','flg_t_open','flg_t_open_opp','flg_t_has_position','flg_r_first','flg_r_open','flg_r_open_opp','flg_r_has_position','amt_p_effective_stack','amt_f_effective_stack','amt_t_effective_stack','amt_r_effective_stack','amt_p_raise_facing','val_p_raise_facing_pct','amt_p_2bet_facing','val_p_2bet_facing_pct','amt_p_3bet_facing','val_p_3bet_facing_pct','amt_p_4bet_facing','val_p_4bet_facing_pct'], 11: ['id_hand','player_name','amt_p_5bet_facing','val_p_5bet_facing_pct','val_p_raise_aggressor_pos','amt_p_raise_made','val_p_raise_made_pct','amt_p_raise_made_2','val_p_raise_made_2_pct','amt_f_bet_facing','val_f_bet_facing_pct','val_f_bet_aggressor_pos','amt_f_bet_made','val_f_bet_made_pct','amt_f_raise_facing','val_f_raise_facing_pct','amt_f_2bet_facing','val_f_2bet_facing_pct','amt_f_3bet_facing','val_f_3bet_facing_pct','amt_f_4bet_facing','val_f_4bet_facing_pct'], 12: ['id_hand','player_name','val_f_raise_aggressor_pos','amt_f_raise_made','val_f_raise_made_2_pct','amt_t_bet_facing','val_t_bet_facing_pct','val_t_bet_aggressor_pos','amt_t_bet_made','val_t_bet_made_pct','amt_t_raise_facing','val_t_raise_facing_pct','amt_t_2bet_facing','val_t_2bet_facing_pct','amt_t_3bet_facing','val_t_3bet_facing_pct','amt_t_4bet_facing','val_t_4bet_facing_pct','val_t_raise_aggressor_pos','amt_r_raise_made_2','val_r_raise_made_2_pct']}

    maxlen = 0
    for i in tables:
        if len(tables[i]) > maxlen:
            maxlen = len(tables[i])

    for i in tables:
        while len(tables[i]) < maxlen:
            tables[i].append(None)

        temp = tables[i][:]

        tables[i] = []
        for field in temp:
            o = Holder(field)
            o.on = False
            tables[i].append(o)


    return DataFrame.from_dict(tables)


def process_fields(df, fields):
    for i in range(len(fields)): # f has chunk, on, field
        f = fields[i]
        curr = df[f.chunk]
        for c in curr:
            if c.field == f.field:
                df.loc[i,f.chunk] = f
    return df

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
                    onmsg = ' (***)'
                else:
                    onmsg = ''
                text = currobj.field + onmsg
                newdf[i].append(text)

    output = DataFrame.from_dict(newdf, orient = 'index')
    print output
    output.columns = range(1,13)

    return output

def make_df(field_df,db,name,summary_path):

    final_df = read_csv(summary_path) # summary path to add to
    final_df.sort('id_hand', inplace = True)
    if 'Unnamed: 0' in final_df.columns:
        del final_df['Unnamed: 0']

    execute = []
    for col in field_df.columns: # chunk
        for row in field_df.index:
            print field_df
            if field_df.loc[row,col].on == True:
                curr = field_df.iloc[row,col].field
                execute.append((col,row))

    dfex = DataFrame(execute)
    dfex.columns = 'Chunk','Field'

    chunks_to_do = set(dfex.Chunk.values)

    for ch in chunks_to_do:
        dfch = read_csv('X:/Users/Josh/Desktop/POKER PROJECT/StageG/db' + str(db) + '_batch' + str(ch) + '.csv') # processed batch
        dfch.sort('id_hand', inplace = True)
        if 'Unnamed: 0' in dfch.columns:
            del dfch['Unnamed: 0']

        keeper_list = []
        # keeper_list.append('id_hand')
        # keeper_list.append('player_name')
        keepers = dfex[dfex.Chunk == ch].Field.values.tolist()
        print keepers
        for k in keepers: # make a list of columns to merge in this bach
            k = field_df.loc[k,ch].field
            final_df[k] = dfch[k].copy().values


        # dfch = dfch[keeper_list]

        # final_df = final_df.merge(dfch, on = ('id_hand','player_name'), how = 'left')

    newpath = 'X:/Users/Josh/Desktop/POKER PROJECT/StageG/df_with_fields/' + name
    if not os.path.exists(newpath): os.makedirs(newpath)
    os.chdir(newpath)

    final_df.to_csv(name + '_' + str(db) + '.csv')
    return final_df
    # final_df.to_csv('X:/Users/Josh/Desktop/POKER PROJECT/StageG/db' + str(db) + '_SUMMARY.csv')

def run(fields):
    df = make_field_df()
    df = process_fields(df, fields)
    # print_df(df).to_csv('X:/Users/Josh/Desktop/POKER PROJECT/StageG/tables_fields.csv')
    return df

### START
os.chdir('X:\Users\Josh\Desktop\POKER PROJECT\StageG')
fields = []

# add_field('amt_before',9)
# add_field('amt_expected_won',3)

add_field('flg_vpip',1)
add_field('cnt_p_raise',1)
add_field('cnt_p_face_limpers',1) # > 0
add_field('amt_expected_won',3)
add_field('flg_p_3bet_opp',4)
add_field('flg_p_4bet_opp',4)
add_field('action_p',9)
add_field('amt_before',9)
add_field('amt_blind',9)
add_field('flg_p_open_opp',9)
add_field('amt_p_raise_facing',10)

df = run(fields)
# TODO: deal with memory issue. maybe process 1 chunk at a time?
name = 'VPIP_PFR'
start = 1
finish = 12
for db in range(start,finish+1):
    summary_path = 'X:/Users/Josh/Desktop/POKER PROJECT/StageG/db' + str(db) + '_SUMMARY.csv'
    vpip_pfr = make_df(df,db,name,summary_path)
    update_main_df(db, vpip_pfr, name, start)

# final groupby

newpath = 'X:/Users/Josh/Desktop/POKER PROJECT/StageG/df_with_fields/' + name
if not os.path.exists(newpath): os.makedirs(newpath)
os.chdir(newpath)

df_all = read_csv('VPIP_PFR_ALL.csv', index_col = 'player_name')

df_all = df_all.groupby('player_name').sum()
df_all.sort('hands', ascending = False, inplace = True)
df['VPIP'] = df.cnt_vpip / df.cnt_vpip_opp * 100
df['PFR'] = df.cnt_pfr / df.cnt_pfr_opp * 100
df['EV_bb'] = df.amt_expected_won / float(2)
df['EV_bb_100'] = df.EV_bb / df.hands * 100
del df['amt_expected_won']

df.EV_bb_100 = df.EV_bb_100.apply(lambda x: "{:.2f}".format(x))


df_all.to_csv('VPIP_PFR_ALL.csv')

## simplify this:
# - rather than a join/marge, just set df['col'] = column_name
