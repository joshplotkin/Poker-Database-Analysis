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
os.chdir('X:\Users\Josh\Desktop\POKER PROJECT\Stage F - BTN v BB\Tables')


# tables = {'cash_hand_histories':['id_hand','history'],'cash_hand_player_combinations':['id_hand','id_gametype','id_player','flg_f_highcard','flg_f_1pair','flg_f_2pair','flg_f_threeoak','flg_f_straight','flg_f_flush','flg_f_fullhouse','flg_f_fouroak','flg_f_strflush','id_f_hand_strength','id_f_kicker_strength','val_f_hole_cards_used','flg_f_gutshot_draw','flg_f_straight_draw','flg_f_2gutshot_draw','flg_f_flush_draw','flg_f_wrap_draw','val_f_wrap_outs','flg_f_bflush_draw','flg_f_bstraight_draw','id_f_flush_draw_strength','val_f_straight_draw_nut_outs','val_f_straight_draw_nonnut_outs','flg_f_low_hand','val_f_low_hand_strength','flg_f_nut_flush_blocker','flg_f_low_draw','val_f_low_draw_strength','flg_t_highcard','flg_t_1pair','flg_t_2pair','flg_t_threeoak','flg_t_straight','flg_t_flush','flg_t_fullhouse','flg_t_fouroak','flg_t_strflush','id_t_hand_strength','id_t_kicker_strength','val_t_hole_cards_used','flg_t_gutshot_draw','flg_t_straight_draw','flg_t_2gutshot_draw','flg_t_flush_draw','flg_t_wrap_draw','val_t_wrap_outs','id_t_flush_draw_strength','val_t_straight_draw_nut_outs','val_t_straight_draw_nonnut_outs','flg_t_low_hand','val_t_low_hand_strength','flg_t_nut_flush_blocker','flg_t_low_draw','val_t_low_draw_strength','flg_r_highcard','flg_r_1pair','flg_r_2pair','flg_r_threeoak','flg_r_straight','flg_r_flush','flg_r_fullhouse','flg_r_fouroak','flg_r_strflush','id_r_hand_strength','id_r_kicker_strength','val_r_hole_cards_used','flg_r_low_hand','val_r_low_hand_strength','flg_r_nut_flush_blocker'],'cash_hand_summary':['id_hand','id_gametype','id_site','id_limit','id_table','hand_no','date_played','date_imported','cnt_players','cnt_players_lookup_position','cnt_players_f','cnt_players_t','cnt_players_r','amt_pot','amt_rake','amt_mgr','amt_pot_p','amt_pot_f','amt_pot_t','amt_pot_r','str_actors_p','str_actors_f','str_actors_t','str_actors_r','str_aggressors_p','str_aggressors_f','str_aggressors_t','str_aggressors_r','id_win_hand','id_win_hand_lo','id_winner','id_winner_lo','button','card_1','card_2','card_3','card_4','card_5','flg_note','flg_tag','flg_autonote'],'cash_limit':['id_limit','limit_name','id_gametype','limit_currency','amt_sb','amt_bb','flg_nlpl','flg_nl','flg_pl','flg_lo','flg_fr','flg_sh','flg_hu','id_filter'],'lookup_actions':['id_action','action'],'lookup_hand_groups':['id_group','group_name'],'lookup_hand_ranks':['id_hand_rank','id_group','group_name','group_details'],'lookup_hole_cards':['id_holecard','id_gametype','hole_cards','enum_pair_type','val_pair_1','val_pair_2','flg_h_suited','flg_h_connector','flg_h_1_gap','flg_h_2_gap','flg_o_connector','flg_o_connector_2_str','flg_o_connector_2_gap','flg_o_connector_3'],'lookup_positions':['cnt_players','position','absolute_position','description','flg_sb','flg_bb','flg_ep','flg_mp','flg_co','flg_btn'],'lookup_sites':['id_site','site_abbrev','site_name'],'lookup_tags':['id_tag','enum_type','tag','icon'],'player':['id_player','id_site','player_name','player_name_search','id_player_alias','flg_note','flg_tag'],'settings':['setting_name','setting_value'],'tags':['id_x','enum_type','flg_tournament','id_tag','flg_auto']}
# for i in range(1,5):
# 	for table in tables:
# 		filename = '15_' + table + '_' + str(i)
# 		df = read_csv(filename+'.csv', encoding='utf-8', index_col = 'Unnamed: 0')
# 		df.columns = tables[table]
# 		df.to_csv(filename+'b.csv', encoding='utf-8')


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

def get_playerstats(db, table, column, mark):

	print db, table

	col_str = ', '.join(column) # turn list into strings so it can be used in SQL statement

	statement = 'SELECT ' + col_str + ' FROM ' + table

	mark.execute(statement)

	playerstats = mark.fetchall()
	playerstats = pd.DataFrame(playerstats)
	playerstats.columns = column

    ## for names in bad formats, convert the unicode errors to * (e.g. euro symbol or screen names)
	fix_name = lambda x: unicode(x, errors = 'replace').replace(u"\ufffd",'*')
	for col in ('player_name', 'player_name_search', 'limit_name'):
		if col in column:
			playerstats[col] = playerstats[col].apply(fix_name)


	playerstats.to_csv('15_' + table + '_' + str(db) + '.csv', encoding='utf-8')


for db in range(2,15):
	mark = login(db)
	tables={'cash_hand_histories':['id_hand','history'],'cash_hand_player_combinations':['id_hand','id_gametype','id_player','flg_f_highcard','flg_f_1pair','flg_f_2pair','flg_f_threeoak','flg_f_straight','flg_f_flush','flg_f_fullhouse','flg_f_fouroak','flg_f_strflush','id_f_hand_strength','id_f_kicker_strength','val_f_hole_cards_used','flg_f_gutshot_draw','flg_f_straight_draw','flg_f_2gutshot_draw','flg_f_flush_draw','flg_f_wrap_draw','val_f_wrap_outs','flg_f_bflush_draw','flg_f_bstraight_draw','id_f_flush_draw_strength','val_f_straight_draw_nut_outs','val_f_straight_draw_nonnut_outs','flg_f_low_hand','val_f_low_hand_strength','flg_f_nut_flush_blocker','flg_f_low_draw','val_f_low_draw_strength','flg_t_highcard','flg_t_1pair','flg_t_2pair','flg_t_threeoak','flg_t_straight','flg_t_flush','flg_t_fullhouse','flg_t_fouroak','flg_t_strflush','id_t_hand_strength','id_t_kicker_strength','val_t_hole_cards_used','flg_t_gutshot_draw','flg_t_straight_draw','flg_t_2gutshot_draw','flg_t_flush_draw','flg_t_wrap_draw','val_t_wrap_outs','id_t_flush_draw_strength','val_t_straight_draw_nut_outs','val_t_straight_draw_nonnut_outs','flg_t_low_hand','val_t_low_hand_strength','flg_t_nut_flush_blocker','flg_t_low_draw','val_t_low_draw_strength','flg_r_highcard','flg_r_1pair','flg_r_2pair','flg_r_threeoak','flg_r_straight','flg_r_flush','flg_r_fullhouse','flg_r_fouroak','flg_r_strflush','id_r_hand_strength','id_r_kicker_strength','val_r_hole_cards_used','flg_r_low_hand','val_r_low_hand_strength','flg_r_nut_flush_blocker'],'cash_hand_summary':['id_hand','id_gametype','id_site','id_limit','id_table','hand_no','date_played','date_imported','cnt_players','cnt_players_lookup_position','cnt_players_f','cnt_players_t','cnt_players_r','amt_pot','amt_rake','amt_mgr','amt_pot_p','amt_pot_f','amt_pot_t','amt_pot_r','str_actors_p','str_actors_f','str_actors_t','str_actors_r','str_aggressors_p','str_aggressors_f','str_aggressors_t','str_aggressors_r','id_win_hand','id_win_hand_lo','id_winner','id_winner_lo','button','card_1','card_2','card_3','card_4','card_5','flg_note','flg_tag','flg_autonote'],'cash_limit':['id_limit','limit_name','id_gametype','limit_currency','amt_sb','amt_bb','flg_nlpl','flg_nl','flg_pl','flg_lo','flg_fr','flg_sh','flg_hu','id_filter'],'lookup_actions':['id_action','action'],'lookup_hand_groups':['id_group','group_name'],'lookup_hand_ranks':['id_hand_rank','id_group','group_name','group_details'],'lookup_hole_cards':['id_holecard','id_gametype','hole_cards','enum_pair_type','val_pair_1','val_pair_2','flg_h_suited','flg_h_connector','flg_h_1_gap','flg_h_2_gap','flg_o_connector','flg_o_connector_2_str','flg_o_connector_2_gap','flg_o_connector_3'],'lookup_positions':['cnt_players','position','absolute_position','description','flg_sb','flg_bb','flg_ep','flg_mp','flg_co','flg_btn'],'lookup_sites':['id_site','site_abbrev','site_name'],'player':['id_player','id_site','player_name','player_name_search','id_player_alias','flg_note','flg_tag']}
	for table in tables:
		get_playerstats(db, table, tables[table], mark)
