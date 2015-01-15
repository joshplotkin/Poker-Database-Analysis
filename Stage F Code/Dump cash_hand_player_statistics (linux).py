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
os.chdir('\media\josh\DATA\Users\Josh\Desktop\POKER PROJECT\StageF - Dump Tables\cash_hand_player_statistics')

def login(number):
	db_name = 'EDAV_'+str(number) # the databases are EDAV_1,...,EDAV_4
	connection = psycopg2.connect("dbname=" + db_name + " user=postgres password=postgrespass")
	mark = connection.cursor()
	return mark

def rename_headers(table_headers, record): # RENAME COLUMNS
	column_names = 'select column_name from information_schema.columns where table_name = ' + table_headers
	mark.execute(column_names)
	headers = mark.fetchall()

	i=0
	for col in headers:
		col = str(col)[2:-3]
		record.rename(columns={i:col}, inplace=True)
		i += 1

def rename_custom(custom_headers, df): # RENAME COLUMNS

	i=0
	for col in custom_headers:
		df.rename(columns={i:col}, inplace=True)
		i += 1

def get_playerstats(db, dumpnumber, column, mark):

	col_str = ', '.join(column) # turn list into strings so it can be used in SQL statement

	statement = 'SELECT ' + col_str + ' FROM cash_hand_player_statistics'

	mark.execute(statement)

	playerstats = mark.fetchall()
	playerstats = pd.DataFrame(playerstats)
	playerstats.columns = column
    # rename_custom(custom_headers, playerstats)

	playerstats.to_csv('16_batch' + str(dumpnumber) + '_' + str(db) + '.csv', encoding='utf-8')


tables = {1:['id_player','id_limit','id_gametype','position','id_player_real','id_holecard','id_session','date_played','cnt_players','cnt_players_lookup_position','cnt_p_raise','flg_p_first_raise','cnt_p_call','flg_p_limp','flg_p_fold','flg_p_ccall','cnt_p_face_limpers','flg_vpip','flg_f_bet','cnt_f_raise','flg_f_first_raise'],2:['id_player','id_limit','cnt_f_call','flg_f_check','flg_f_check_raise','flg_f_fold','flg_f_saw','flg_t_bet','cnt_t_raise','flg_t_first_raise','cnt_t_call','flg_t_check','flg_t_check_raise','flg_t_fold','flg_t_saw','flg_r_bet','cnt_r_raise','flg_r_first_raise','cnt_r_call','flg_r_check','flg_r_check_raise','flg_r_fold'],3:['id_player','id_limit','flg_r_saw','enum_allin','enum_face_allin','enum_face_allin_action','flg_blind_s','flg_blind_b','flg_blind_ds','flg_blind_db','flg_sb_steal_fold','flg_bb_steal_fold','flg_blind_def_opp','flg_steal_att','flg_steal_opp','flg_blind_k','flg_showdown','flg_won_hand','amt_won','amt_expected_won','val_equity','amt_r_raise_made'],4:['id_player','id_limit','id_final_hand','id_final_hand_lo','flg_showed','enum_folded','flg_p_face_raise','flg_p_3bet','flg_p_3bet_opp','flg_p_3bet_def_opp','enum_p_3bet_action','flg_p_4bet','flg_p_4bet_opp','flg_p_4bet_def_opp','enum_p_4bet_action','flg_p_squeeze','flg_p_squeeze_opp','flg_p_squeeze_def_opp','enum_p_squeeze_action','flg_f_face_raise','flg_f_3bet'],5:['id_player','id_limit','flg_f_3bet_opp','flg_f_3bet_def_opp','enum_f_3bet_action','flg_f_4bet','flg_f_4bet_opp','flg_f_4bet_def_opp','enum_f_4bet_action','flg_f_cbet','flg_f_cbet_opp','flg_f_cbet_def_opp','enum_f_cbet_action','flg_t_face_raise','flg_t_3bet','flg_t_3bet_opp','flg_t_3bet_def_opp','enum_t_3bet_action','flg_t_4bet','flg_t_4bet_opp','flg_t_4bet_def_opp','enum_t_4bet_action'],6:['id_player','id_limit','flg_t_cbet','flg_t_cbet_opp','amt_t_raise_made','val_t_raise_made_pct','amt_t_raise_made_2','val_t_raise_made_2_pct','amt_r_bet_facing','val_r_bet_facing_pct','val_r_bet_aggressor_pos','amt_r_bet_made','val_r_bet_made_pct','amt_r_raise_facing','val_r_raise_facing_pct','amt_r_2bet_facing','val_r_2bet_facing_pct','amt_r_3bet_facing','val_r_3bet_facing_pct','amt_r_4bet_facing','val_r_4bet_facing_pct','val_r_raise_aggressor_pos'],7:['id_player','id_limit','flg_t_cbet_def_opp','enum_t_cbet_action','flg_t_float','flg_t_float_opp','flg_t_float_def_opp','enum_t_float_action','flg_t_donk','flg_t_donk_opp','flg_t_donk_def_opp','enum_t_donk_action','flg_r_face_raise','flg_r_3bet','flg_r_3bet_opp','flg_r_3bet_def_opp','enum_r_3bet_action','flg_r_4bet','flg_r_4bet_opp','flg_r_4bet_def_opp','enum_r_4bet_action'],8:['id_player','id_limit','flg_r_cbet','flg_r_cbet_opp','flg_r_cbet_def_opp','enum_r_cbet_action','flg_r_float','flg_r_float_opp','flg_r_float_def_opp','enum_r_float_action','flg_r_donk','flg_r_donk_opp','flg_r_donk_def_opp','enum_r_donk_action','val_curr_conv','seat','holecard_1','holecard_2','holecard_3','holecard_4','id_suits','flg_hero'],9:['id_player','id_limit','amt_before','amt_blind','amt_ante','amt_bet_p','amt_bet_f','amt_bet_t','amt_bet_r','amt_bet_ttl','id_action_p','id_action_f','id_action_t','id_action_r','flg_p_open','flg_p_open_opp','flg_f_first','flg_f_open','flg_f_open_opp','flg_f_has_position','flg_t_first','val_r_raise_made_pct'],10:['id_player','id_limit','flg_t_open','flg_t_open_opp','flg_t_has_position','flg_r_first','flg_r_open','flg_r_open_opp','flg_r_has_position','amt_p_effective_stack','amt_f_effective_stack','amt_t_effective_stack','amt_r_effective_stack','amt_p_raise_facing','val_p_raise_facing_pct','amt_p_2bet_facing','val_p_2bet_facing_pct','amt_p_3bet_facing','val_p_3bet_facing_pct','amt_p_4bet_facing','val_p_4bet_facing_pct'],11:['id_player','id_limit','amt_p_5bet_facing','val_p_5bet_facing_pct','val_p_raise_aggressor_pos','amt_p_raise_made','val_p_raise_made_pct','amt_p_raise_made_2','val_p_raise_made_2_pct','amt_f_bet_facing','val_f_bet_facing_pct','val_f_bet_aggressor_pos','amt_f_bet_made','val_f_bet_made_pct','amt_f_raise_facing','val_f_raise_facing_pct','amt_f_2bet_facing','val_f_2bet_facing_pct','amt_f_3bet_facing','val_f_3bet_facing_pct','amt_f_4bet_facing','val_f_4bet_facing_pct'],12:['id_player','id_limit','val_f_raise_aggressor_pos','amt_f_raise_made','val_f_raise_made_2_pct','amt_t_bet_facing','val_t_bet_facing_pct','val_t_bet_aggressor_pos','amt_t_bet_made','val_t_bet_made_pct','amt_t_raise_facing','val_t_raise_facing_pct','amt_t_2bet_facing','val_t_2bet_facing_pct','amt_t_3bet_facing','val_t_3bet_facing_pct','amt_t_4bet_facing','val_t_4bet_facing_pct','val_t_raise_aggressor_pos','amt_r_raise_made_2','val_r_raise_made_2_pct']}



for db in range(6,15):
	mark = login(db)
	for dumpnumber in tables:
			if np.logical_and(db != 4, db != 1):
				get_playerstats(db, dumpnumber, tables[dumpnumber], mark)

# for db,dumpnumber in [(2,39)]:
# 	mark = login(db)
# 	get_playerstats(db, dumpnumber, tables[dumpnumber], mark)
