import pandas as pd
import numpy as np
from pandas import *
import os
import psycopg2
import sys
import brewer2mpl
import pylab as pl
import matplotlib as mpl
import matplotlib
import matplotlib.pyplot as plt
import scipy as sc
from scipy import stats
from scipy.stats import norm
from pylab import *
from numpy import float64
from mpltools import style
from mpltools import special
import statsmodels.api as sm
from statsmodels.sandbox.regression.predstd import wls_prediction_std
import statsmodels.formula.api as smf
from statsmodels.formula.api import ols
from sklearn import mixture
import random
from sklearn.cluster import KMeans
from matplotlib.ticker import NullFormatter
from sklearn.neighbors.kde import KernelDensity
import mpld3
from mpld3 import plugins, utils

style.use('ggplot')

pd.set_option('max_rows',4000)
pd.set_option('max_columns',1000)
pandas.set_option('precision',3)
os.chdir('c:/users/jp/desktop/dropbox/school/spring2014/edav/Poker data/NEW_TRY/StageD')


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

def get_playerstats(number, dumpnumber):

    mark = login(number)
    distinct = ''
    table = 'cash_cache'
    
    if dumpnumber == 1:
	    column = 'id_player,id_gametype,id_limit,cnt_players,position_type,flg_has_position_f,flg_has_position_t,flg_has_position_r,date_played_year_week,amt_bb_won,amt_blind,amt_blind_curr_conv,amt_expected_bb_won,amt_expected_won,amt_expected_won_curr_conv,amt_r_call,amt_r_call_won,amt_rake_attributed,amt_rake_attributed_curr_conv,amt_rake_share,amt_rake_share_curr_conv,amt_rake_taken,amt_rake_taken_curr_conv,amt_rake_weighted_contributed,amt_rake_weighted_contributed_curr_conv,amt_won,amt_won_curr_conv,cnt_blind,cnt_f_2bet_def_action_call,cnt_f_2bet_def_action_fold,cnt_f_3bet,cnt_f_3bet_def_action_call,cnt_f_3bet_def_action_fold,cnt_f_3bet_def_opp,cnt_f_3bet_opp,cnt_f_4bet,cnt_f_4bet_def_action_call,cnt_f_4bet_def_action_fold,cnt_f_4bet_def_opp,cnt_f_4bet_opp,cnt_f_allin,cnt_f_allin_valid,cnt_f_bet,cnt_f_bet_def_action_call,cnt_f_bet_def_action_fold,cnt_f_bet_def_action_raise,cnt_f_bet_def_opp,cnt_f_bet_limp_pot,cnt_f_call,cnt_f_cbet,cnt_f_cbet_3bet_pot,cnt_f_cbet_def_action_call,cnt_f_cbet_def_action_call_in_pos,cnt_f_cbet_def_action_call_out_of_pos,cnt_f_cbet_def_action_fold,cnt_f_cbet_def_action_fold_in_pos,cnt_f_cbet_def_action_fold_out_of_pos,cnt_f_cbet_def_action_raise,cnt_f_cbet_def_action_raise_in_pos,cnt_f_cbet_def_action_raise_out_of_pos,cnt_f_cbet_def_opp,cnt_f_cbet_face_raise,cnt_f_cbet_face_raise_3bet_pot,cnt_f_cbet_fold_to_raise,cnt_f_cbet_fold_to_raise_3bet_pot,cnt_f_cbet_opp,cnt_f_cbet_opp_3bet_pot,cnt_f_cbet_success,cnt_f_check,cnt_f_check_raise,cnt_f_check_raise_opp,cnt_f_donk,cnt_f_donk_3bet_pot,cnt_f_donk_def_opp,cnt_f_donk_def_opp_3bet_pot,cnt_f_donk_def_opp_action_call,cnt_f_donk_def_opp_action_fold,cnt_f_donk_def_opp_action_fold_3bet_pot,cnt_f_donk_def_opp_action_raise,cnt_f_donk_def_opp_action_raise_3bet_pot,cnt_f_donk_face_raise,cnt_f_donk_fold,cnt_f_donk_opp,cnt_f_donk_opp_3bet_pot,cnt_f_face_xr,cnt_f_face_xr_call,cnt_f_face_xr_fold,cnt_f_face_xr_raise,cnt_f_float,cnt_f_float_3bet_pot,cnt_f_float_def_opp,cnt_f_float_def_opp_action_call,cnt_f_float_def_opp_action_fold,cnt_f_float_def_opp_action_raise,cnt_f_float_face_raise,cnt_f_float_fold,cnt_f_float_opp,cnt_f_float_opp_3bet_pot,cnt_f_fold,cnt_f_open_opp_limp_pot,cnt_f_raise,cnt_f_raise_def_opp,cnt_f_saw,cnt_f_saw_won,cnt_hands,cnt_hands_won,cnt_p_2bet_def_action_call,cnt_p_2bet_def_action_fold,cnt_p_2bet_def_opp,cnt_p_3bet,cnt_p_3bet_def_action_call,cnt_p_3bet_def_action_fold,cnt_p_3bet_def_action_fold_when_open_raised,cnt_p_3bet_def_action_fold_when_open_raised_ip,cnt_p_3bet_def_action_fold_when_open_raised_oop,cnt_p_3bet_def_opp,cnt_p_3bet_def_opp_when_open_raised,cnt_p_3bet_def_opp_when_open_raised_ip,cnt_p_3bet_def_opp_when_open_raised_oop,cnt_p_3bet_f_cbet_def_action_call,cnt_p_3bet_f_cbet_def_action_fold,cnt_p_3bet_f_cbet_def_action_raise,cnt_p_3bet_f_cbet_def_opp,cnt_p_3bet_opp,cnt_p_3bet_opp_vs_bb_2bet,cnt_p_3bet_opp_vs_btn_2bet,cnt_p_3bet_opp_vs_btn_open,cnt_p_3bet_opp_vs_co_2bet,cnt_p_3bet_opp_vs_co_open,cnt_p_3bet_opp_vs_ep_2bet,cnt_p_3bet_opp_vs_mp_2bet,cnt_p_3bet_opp_vs_sb_2bet,cnt_p_3bet_r_cbet_def_action_call,cnt_p_3bet_r_cbet_def_action_fold,cnt_p_3bet_r_cbet_def_action_raise,cnt_p_3bet_r_cbet_def_opp,cnt_p_3bet_success,cnt_p_3bet_t_cbet_def_action_call,cnt_p_3bet_t_cbet_def_action_fold,cnt_p_3bet_t_cbet_def_action_raise,cnt_p_3bet_t_cbet_def_opp,cnt_p_3bet_vs_bb_2bet,cnt_p_3bet_vs_btn_2bet,cnt_p_3bet_vs_btn_open,cnt_p_3bet_vs_co_2bet,cnt_p_3bet_vs_co_open,cnt_p_3bet_vs_ep_2bet,cnt_p_3bet_vs_mp_2bet,cnt_p_3bet_vs_sb_2bet,cnt_p_4bet,cnt_p_4bet_after_raising,cnt_p_4bet_def_action_call,cnt_p_4bet_def_action_fold,cnt_p_4bet_def_action_fold_after_3b,cnt_p_4bet_def_opp,cnt_p_4bet_def_opp_after_3b,cnt_p_4bet_opp,cnt_p_4bet_opp_when_open_raised,cnt_p_5bet,cnt_p_5bet_def_action_fold,cnt_p_5bet_def_opp,cnt_p_5bet_opp,cnt_p_allin,cnt_p_allin_valid,cnt_p_bb_v_sb_3bet,cnt_p_bb_v_sb_call,cnt_p_bb_v_sb_fold,cnt_p_call,cnt_p_call_vs_btn_open,cnt_p_call_vs_co_open,cnt_p_ccall,cnt_p_ccall_3bet,cnt_p_ccall_3bet_opp,cnt_p_ccall_opp,cnt_p_facing_limpers,cnt_p_fold,cnt_p_limp,cnt_p_limp_call,cnt_p_limp_faceraise,cnt_p_limp_fold,cnt_p_limp_opp,cnt_p_limp_raise,cnt_p_open_opp,cnt_p_open_opp_btn,cnt_p_open_opp_ep,cnt_p_raise,cnt_p_raise_3bet,cnt_p_raise_first_in,cnt_p_raise_limpers,cnt_p_rfi_btn,cnt_p_rfi_ep,cnt_p_squeeze,cnt_p_squeeze_def_call,cnt_p_squeeze_def_fold,cnt_p_squeeze_def_opp,cnt_p_squeeze_def_raise,cnt_p_squeeze_opp,cnt_pfr,cnt_pfr_opp,cnt_players_ttl'
	    custom_headers = ('PLAYER_ID','id_gametype','LIMIT','cnt_players','position_type','flg_has_position_f','flg_has_position_t','flg_has_position_r','date_played_year_week','amt_bb_won','amt_blind','amt_blind_curr_conv','amt_expected_bb_won','amt_expected_won','amt_expected_won_curr_conv','amt_r_call','amt_r_call_won','amt_rake_attributed','amt_rake_attributed_curr_conv','amt_rake_share','amt_rake_share_curr_conv','amt_rake_taken','amt_rake_taken_curr_conv','amt_rake_weighted_contributed','amt_rake_weighted_contributed_curr_conv','amt_won','amt_won_curr_conv','cnt_blind','cnt_f_2bet_def_action_call','cnt_f_2bet_def_action_fold','cnt_f_3bet','cnt_f_3bet_def_action_call','cnt_f_3bet_def_action_fold','cnt_f_3bet_def_opp','cnt_f_3bet_opp','cnt_f_4bet','cnt_f_4bet_def_action_call','cnt_f_4bet_def_action_fold','cnt_f_4bet_def_opp','cnt_f_4bet_opp','cnt_f_allin','cnt_f_allin_valid','cnt_f_bet','cnt_f_bet_def_action_call','cnt_f_bet_def_action_fold','cnt_f_bet_def_action_raise','cnt_f_bet_def_opp','cnt_f_bet_limp_pot','cnt_f_call','cnt_f_cbet','cnt_f_cbet_3bet_pot','cnt_f_cbet_def_action_call','cnt_f_cbet_def_action_call_in_pos','cnt_f_cbet_def_action_call_out_of_pos','cnt_f_cbet_def_action_fold','cnt_f_cbet_def_action_fold_in_pos','cnt_f_cbet_def_action_fold_out_of_pos','cnt_f_cbet_def_action_raise','cnt_f_cbet_def_action_raise_in_pos','cnt_f_cbet_def_action_raise_out_of_pos','cnt_f_cbet_def_opp','cnt_f_cbet_face_raise','cnt_f_cbet_face_raise_3bet_pot','cnt_f_cbet_fold_to_raise','cnt_f_cbet_fold_to_raise_3bet_pot','cnt_f_cbet_opp','cnt_f_cbet_opp_3bet_pot','cnt_f_cbet_success','cnt_f_check','cnt_f_check_raise','cnt_f_check_raise_opp','cnt_f_donk','cnt_f_donk_3bet_pot','cnt_f_donk_def_opp','cnt_f_donk_def_opp_3bet_pot','cnt_f_donk_def_opp_action_call','cnt_f_donk_def_opp_action_fold','cnt_f_donk_def_opp_action_fold_3bet_pot','cnt_f_donk_def_opp_action_raise','cnt_f_donk_def_opp_action_raise_3bet_pot','cnt_f_donk_face_raise','cnt_f_donk_fold','cnt_f_donk_opp','cnt_f_donk_opp_3bet_pot','cnt_f_face_xr','cnt_f_face_xr_call','cnt_f_face_xr_fold','cnt_f_face_xr_raise','cnt_f_float','cnt_f_float_3bet_pot','cnt_f_float_def_opp','cnt_f_float_def_opp_action_call','cnt_f_float_def_opp_action_fold','cnt_f_float_def_opp_action_raise','cnt_f_float_face_raise','cnt_f_float_fold','cnt_f_float_opp','cnt_f_float_opp_3bet_pot','cnt_f_fold','cnt_f_open_opp_limp_pot','cnt_f_raise','cnt_f_raise_def_opp','cnt_f_saw','cnt_f_saw_won','cnt_hands','cnt_hands_won','cnt_p_2bet_def_action_call','cnt_p_2bet_def_action_fold','cnt_p_2bet_def_opp','cnt_p_3bet','cnt_p_3bet_def_action_call','cnt_p_3bet_def_action_fold','cnt_p_3bet_def_action_fold_when_open_raised','cnt_p_3bet_def_action_fold_when_open_raised_ip','cnt_p_3bet_def_action_fold_when_open_raised_oop','cnt_p_3bet_def_opp','cnt_p_3bet_def_opp_when_open_raised','cnt_p_3bet_def_opp_when_open_raised_ip','cnt_p_3bet_def_opp_when_open_raised_oop','cnt_p_3bet_f_cbet_def_action_call','cnt_p_3bet_f_cbet_def_action_fold','cnt_p_3bet_f_cbet_def_action_raise','cnt_p_3bet_f_cbet_def_opp','cnt_p_3bet_opp','cnt_p_3bet_opp_vs_bb_2bet','cnt_p_3bet_opp_vs_btn_2bet','cnt_p_3bet_opp_vs_btn_open','cnt_p_3bet_opp_vs_co_2bet','cnt_p_3bet_opp_vs_co_open','cnt_p_3bet_opp_vs_ep_2bet','cnt_p_3bet_opp_vs_mp_2bet','cnt_p_3bet_opp_vs_sb_2bet','cnt_p_3bet_r_cbet_def_action_call','cnt_p_3bet_r_cbet_def_action_fold','cnt_p_3bet_r_cbet_def_action_raise','cnt_p_3bet_r_cbet_def_opp','cnt_p_3bet_success','cnt_p_3bet_t_cbet_def_action_call','cnt_p_3bet_t_cbet_def_action_fold','cnt_p_3bet_t_cbet_def_action_raise','cnt_p_3bet_t_cbet_def_opp','cnt_p_3bet_vs_bb_2bet','cnt_p_3bet_vs_btn_2bet','cnt_p_3bet_vs_btn_open','cnt_p_3bet_vs_co_2bet','cnt_p_3bet_vs_co_open','cnt_p_3bet_vs_ep_2bet','cnt_p_3bet_vs_mp_2bet','cnt_p_3bet_vs_sb_2bet','cnt_p_4bet','cnt_p_4bet_after_raising','cnt_p_4bet_def_action_call','cnt_p_4bet_def_action_fold','cnt_p_4bet_def_action_fold_after_3b','cnt_p_4bet_def_opp','cnt_p_4bet_def_opp_after_3b','cnt_p_4bet_opp','cnt_p_4bet_opp_when_open_raised','cnt_p_5bet','cnt_p_5bet_def_action_fold','cnt_p_5bet_def_opp','cnt_p_5bet_opp','cnt_p_allin','cnt_p_allin_valid','cnt_p_bb_v_sb_3bet','cnt_p_bb_v_sb_call','cnt_p_bb_v_sb_fold','cnt_p_call','cnt_p_call_vs_btn_open','cnt_p_call_vs_co_open','cnt_p_ccall','cnt_p_ccall_3bet','cnt_p_ccall_3bet_opp','cnt_p_ccall_opp','cnt_p_facing_limpers','cnt_p_fold','cnt_p_limp','cnt_p_limp_call','cnt_p_limp_faceraise','cnt_p_limp_fold','cnt_p_limp_opp','cnt_p_limp_raise','cnt_p_open_opp','cnt_p_open_opp_btn','cnt_p_open_opp_ep','cnt_p_raise','cnt_p_raise_3bet','cnt_p_raise_first_in','cnt_p_raise_limpers','cnt_p_rfi_btn','cnt_p_rfi_ep','cnt_p_squeeze','cnt_p_squeeze_def_call','cnt_p_squeeze_def_fold','cnt_p_squeeze_def_opp','cnt_p_squeeze_def_raise','cnt_p_squeeze_opp','cnt_pfr','cnt_pfr_opp','cnt_players_ttl')
    else:
	    column = 'id_player,id_limit,cnt_prev_callers_limp,cnt_r_2bet_def_action_call,cnt_r_2bet_def_action_fold,cnt_r_3bet,cnt_r_3bet_def_action_call,cnt_r_3bet_def_action_fold,cnt_r_3bet_def_opp,cnt_r_3bet_opp,cnt_r_4bet,cnt_r_4bet_def_action_call,cnt_r_4bet_def_action_fold,cnt_r_4bet_def_opp,cnt_r_4bet_opp,cnt_r_allin,cnt_r_allin_valid,cnt_r_bet,cnt_r_bet_def_action_call,cnt_r_bet_def_action_fold,cnt_r_bet_def_action_raise,cnt_r_bet_def_opp,cnt_r_call,cnt_r_call_hands,cnt_r_cbet,cnt_r_cbet_3bet_pot,cnt_r_cbet_def_action_call,cnt_r_cbet_def_action_fold,cnt_r_cbet_def_action_raise,cnt_r_cbet_def_opp,cnt_r_cbet_face_raise,cnt_r_cbet_face_raise_3bet_pot,cnt_r_cbet_fold_to_raise,cnt_r_cbet_fold_to_raise_3bet_pot,cnt_r_cbet_opp,cnt_r_cbet_opp_3bet_pot,cnt_r_cbet_success,cnt_r_check,cnt_r_check_raise,cnt_r_check_raise_opp,cnt_r_donk,cnt_r_donk_3bet_pot,cnt_r_donk_def_action_call,cnt_r_donk_def_action_fold,cnt_r_donk_def_action_fold_3bet_pot,cnt_r_donk_def_action_raise,cnt_r_donk_def_opp,cnt_r_donk_def_opp_3bet_pot,cnt_r_donk_face_raise,cnt_r_donk_fold,cnt_r_donk_opp,cnt_r_donk_opp_3bet_pot,cnt_r_face_xr,cnt_r_face_xr_call,cnt_r_face_xr_fold,cnt_r_face_xr_raise,cnt_r_float,cnt_r_float_3bet_pot,cnt_r_float_def_action_call,cnt_r_float_def_action_fold,cnt_r_float_def_action_raise,cnt_r_float_def_opp,cnt_r_float_face_raise,cnt_r_float_fold,cnt_r_float_opp,cnt_r_float_opp_3bet_pot,cnt_r_fold,cnt_r_probe,cnt_r_probe_3bet_pot,cnt_r_probe_def_action_call,cnt_r_probe_def_action_fold,cnt_r_probe_def_action_raise,cnt_r_probe_def_opp,cnt_r_probe_face_raise,cnt_r_probe_fold,cnt_r_probe_opp,cnt_r_probe_opp_3bet_pot,cnt_r_raise,cnt_r_raise_wtsd,cnt_r_raise_wtsd_won,cnt_r_saw,cnt_steal_3bet_def_action_fold,cnt_steal_3bet_def_opp,cnt_steal_att,cnt_steal_att_lp,cnt_steal_att_sb,cnt_steal_def_action_call,cnt_steal_def_action_call_vs_lp,cnt_steal_def_action_fold,cnt_steal_def_action_fold_vs_lp,cnt_steal_def_action_raise,cnt_steal_def_action_raise_vs_lp,cnt_steal_def_opp,cnt_steal_def_opp_vs_lp,cnt_steal_opp,cnt_steal_opp_lp,cnt_steal_opp_sb,cnt_steal_reraise_def_action_fold,cnt_steal_reraise_def_opp,cnt_steal_success,cnt_t_2bet_def_action_call,cnt_t_2bet_def_action_fold,cnt_t_3bet,cnt_t_3bet_def_action_call,cnt_t_3bet_def_action_fold,cnt_t_3bet_def_opp,cnt_t_3bet_opp,cnt_t_4bet,cnt_t_4bet_def_action_call,cnt_t_4bet_def_action_fold,cnt_t_4bet_def_opp,cnt_t_4bet_opp,cnt_t_allin,cnt_t_allin_valid,cnt_t_bet,cnt_t_bet_def_action_call,cnt_t_bet_def_action_fold,cnt_t_bet_def_action_raise,cnt_t_bet_def_opp,cnt_t_call,cnt_t_cbet,cnt_t_cbet_3bet_pot,cnt_t_cbet_def_action_call,cnt_t_cbet_def_action_fold,cnt_t_cbet_def_action_raise,cnt_t_cbet_def_opp,cnt_t_cbet_face_raise,cnt_t_cbet_face_raise_3bet_pot,cnt_t_cbet_fold_to_raise,cnt_t_cbet_fold_to_raise_3bet_pot,cnt_t_cbet_opp,cnt_t_cbet_opp_3bet_pot,cnt_t_cbet_success,cnt_t_check,cnt_t_check_raise,cnt_t_check_raise_opp,cnt_t_donk,cnt_t_donk_3bet_pot,cnt_t_donk_def_action_call,cnt_t_donk_def_action_fold,cnt_t_donk_def_action_fold_3bet_pot,cnt_t_donk_def_action_raise,cnt_t_donk_def_opp,cnt_t_donk_def_opp_3bet_pot,cnt_t_donk_face_raise,cnt_t_donk_fold,cnt_t_donk_opp,cnt_t_donk_opp_3bet_pot,cnt_t_face_xr,cnt_t_face_xr_call,cnt_t_face_xr_fold,cnt_t_face_xr_raise,cnt_t_float,cnt_t_float_3bet_pot,cnt_t_float_def_action_call,cnt_t_float_def_action_fold,cnt_t_float_def_action_raise,cnt_t_float_def_opp,cnt_t_float_face_raise,cnt_t_float_fold,cnt_t_float_opp,cnt_t_float_opp_3bet_pot,cnt_t_fold,cnt_t_probe,cnt_t_probe_3bet_pot,cnt_t_probe_def_action_call,cnt_t_probe_def_action_fold,cnt_t_probe_def_action_raise,cnt_t_probe_def_opp,cnt_t_probe_face_raise,cnt_t_probe_fold,cnt_t_probe_opp,cnt_t_probe_opp_3bet_pot,cnt_t_raise,cnt_t_raise_wtsd,cnt_t_raise_wtsd_won,cnt_t_saw,cnt_vpip,cnt_vpip_sb,cnt_walks,cnt_won_hand,cnt_wtsd,cnt_wtsd_allin,cnt_wtsd_non_small,cnt_wtsd_won,cnt_wtsd_won_non_small,cnt_wtsd_won_when_r_call,limit_currency,val_equity,val_f_equity,val_p_equity,val_r_equity,val_t_equity'
	    custom_headers = ('PLAYER_ID','LIMIT','cnt_prev_callers_limp','cnt_r_2bet_def_action_call','cnt_r_2bet_def_action_fold','cnt_r_3bet','cnt_r_3bet_def_action_call','cnt_r_3bet_def_action_fold','cnt_r_3bet_def_opp','cnt_r_3bet_opp','cnt_r_4bet','cnt_r_4bet_def_action_call','cnt_r_4bet_def_action_fold','cnt_r_4bet_def_opp','cnt_r_4bet_opp','cnt_r_allin','cnt_r_allin_valid','cnt_r_bet','cnt_r_bet_def_action_call','cnt_r_bet_def_action_fold','cnt_r_bet_def_action_raise','cnt_r_bet_def_opp','cnt_r_call','cnt_r_call_hands','cnt_r_cbet','cnt_r_cbet_3bet_pot','cnt_r_cbet_def_action_call','cnt_r_cbet_def_action_fold','cnt_r_cbet_def_action_raise','cnt_r_cbet_def_opp','cnt_r_cbet_face_raise','cnt_r_cbet_face_raise_3bet_pot','cnt_r_cbet_fold_to_raise','cnt_r_cbet_fold_to_raise_3bet_pot','cnt_r_cbet_opp','cnt_r_cbet_opp_3bet_pot','cnt_r_cbet_success','cnt_r_check','cnt_r_check_raise','cnt_r_check_raise_opp','cnt_r_donk','cnt_r_donk_3bet_pot','cnt_r_donk_def_action_call','cnt_r_donk_def_action_fold','cnt_r_donk_def_action_fold_3bet_pot','cnt_r_donk_def_action_raise','cnt_r_donk_def_opp','cnt_r_donk_def_opp_3bet_pot','cnt_r_donk_face_raise','cnt_r_donk_fold','cnt_r_donk_opp','cnt_r_donk_opp_3bet_pot','cnt_r_face_xr','cnt_r_face_xr_call','cnt_r_face_xr_fold','cnt_r_face_xr_raise','cnt_r_float','cnt_r_float_3bet_pot','cnt_r_float_def_action_call','cnt_r_float_def_action_fold','cnt_r_float_def_action_raise','cnt_r_float_def_opp','cnt_r_float_face_raise','cnt_r_float_fold','cnt_r_float_opp','cnt_r_float_opp_3bet_pot','cnt_r_fold','cnt_r_probe','cnt_r_probe_3bet_pot','cnt_r_probe_def_action_call','cnt_r_probe_def_action_fold','cnt_r_probe_def_action_raise','cnt_r_probe_def_opp','cnt_r_probe_face_raise','cnt_r_probe_fold','cnt_r_probe_opp','cnt_r_probe_opp_3bet_pot','cnt_r_raise','cnt_r_raise_wtsd','cnt_r_raise_wtsd_won','cnt_r_saw','cnt_steal_3bet_def_action_fold','cnt_steal_3bet_def_opp','cnt_steal_att','cnt_steal_att_lp','cnt_steal_att_sb','cnt_steal_def_action_call','cnt_steal_def_action_call_vs_lp','cnt_steal_def_action_fold','cnt_steal_def_action_fold_vs_lp','cnt_steal_def_action_raise','cnt_steal_def_action_raise_vs_lp','cnt_steal_def_opp','cnt_steal_def_opp_vs_lp','cnt_steal_opp','cnt_steal_opp_lp','cnt_steal_opp_sb','cnt_steal_reraise_def_action_fold','cnt_steal_reraise_def_opp','cnt_steal_success','cnt_t_2bet_def_action_call','cnt_t_2bet_def_action_fold','cnt_t_3bet','cnt_t_3bet_def_action_call','cnt_t_3bet_def_action_fold','cnt_t_3bet_def_opp','cnt_t_3bet_opp','cnt_t_4bet','cnt_t_4bet_def_action_call','cnt_t_4bet_def_action_fold','cnt_t_4bet_def_opp','cnt_t_4bet_opp','cnt_t_allin','cnt_t_allin_valid','cnt_t_bet','cnt_t_bet_def_action_call','cnt_t_bet_def_action_fold','cnt_t_bet_def_action_raise','cnt_t_bet_def_opp','cnt_t_call','cnt_t_cbet','cnt_t_cbet_3bet_pot','cnt_t_cbet_def_action_call','cnt_t_cbet_def_action_fold','cnt_t_cbet_def_action_raise','cnt_t_cbet_def_opp','cnt_t_cbet_face_raise','cnt_t_cbet_face_raise_3bet_pot','cnt_t_cbet_fold_to_raise','cnt_t_cbet_fold_to_raise_3bet_pot','cnt_t_cbet_opp','cnt_t_cbet_opp_3bet_pot','cnt_t_cbet_success','cnt_t_check','cnt_t_check_raise','cnt_t_check_raise_opp','cnt_t_donk','cnt_t_donk_3bet_pot','cnt_t_donk_def_action_call','cnt_t_donk_def_action_fold','cnt_t_donk_def_action_fold_3bet_pot','cnt_t_donk_def_action_raise','cnt_t_donk_def_opp','cnt_t_donk_def_opp_3bet_pot','cnt_t_donk_face_raise','cnt_t_donk_fold','cnt_t_donk_opp','cnt_t_donk_opp_3bet_pot','cnt_t_face_xr','cnt_t_face_xr_call','cnt_t_face_xr_fold','cnt_t_face_xr_raise','cnt_t_float','cnt_t_float_3bet_pot','cnt_t_float_def_action_call','cnt_t_float_def_action_fold','cnt_t_float_def_action_raise','cnt_t_float_def_opp','cnt_t_float_face_raise','cnt_t_float_fold','cnt_t_float_opp','cnt_t_float_opp_3bet_pot','cnt_t_fold','cnt_t_probe','cnt_t_probe_3bet_pot','cnt_t_probe_def_action_call','cnt_t_probe_def_action_fold','cnt_t_probe_def_action_raise','cnt_t_probe_def_opp','cnt_t_probe_face_raise','cnt_t_probe_fold','cnt_t_probe_opp','cnt_t_probe_opp_3bet_pot','cnt_t_raise','cnt_t_raise_wtsd','cnt_t_raise_wtsd_won','cnt_t_saw','cnt_vpip','cnt_vpip_sb','cnt_walks','cnt_won_hand','cnt_wtsd','cnt_wtsd_allin','cnt_wtsd_non_small','cnt_wtsd_won','cnt_wtsd_won_non_small','cnt_wtsd_won_when_r_call','limit_currency','val_equity','val_f_equity','val_p_equity','val_r_equity','val_t_equity')



    statement = 'SELECT ' + column + ' FROM ' + table
    
    mark.execute(statement)
    
    playerstats = mark.fetchall()
    playerstats = pd.DataFrame(playerstats)
    rename_custom(custom_headers, playerstats)
    
    ## LIMIT IDs
    statement = 'SELECT id_limit,amt_bb FROM cash_limit'
    
    mark.execute(statement)
    limits = mark.fetchall()
    limits = pd.DataFrame(limits)
    
    custom_headers = ('LIMIT','BB_SIZE')
    rename_custom(custom_headers, limits)

    
    ## PLAYER NAMES
    statement = 'SELECT id_player, player_name FROM player'
    mark.execute(statement)
    screennames = mark.fetchall()
    screennames = pd.DataFrame(screennames)
    
    custom_headers = ('PLAYER_ID', 'NAME')
    rename_custom(custom_headers, screennames)

    fix_name = lambda x: unicode(x, errors = 'replace').replace(u"\ufffd",'*')
    screennames.NAME = screennames.NAME.apply(fix_name)

    limits = limits.set_index('LIMIT')
    playerstats = playerstats.join(limits, on = 'LIMIT', how = 'left')
    playerstats = playerstats[np.logical_or(playerstats.BB_SIZE == 1, playerstats.BB_SIZE == 2)]

    screennames = screennames.set_index('PLAYER_ID')
    playerstats = playerstats.groupby('PLAYER_ID').sum()

    playerstats = playerstats.join(screennames, how = 'left')
    playerstats = playerstats.set_index('NAME')

    del playerstats['LIMIT']

    playerstats.to_csv('12_SHOWDOWN_' + str(number) + '_' + str(dumpnumber) + '.csv')

for db in range(1,5):
    for chunk in range(1,3):
        get_playerstats(db,chunk)
