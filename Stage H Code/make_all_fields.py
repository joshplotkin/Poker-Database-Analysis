import numpy as np
import os
import pandas as pd
from pandas import *

os.chdir('X:/Users/Josh/Desktop/POKER PROJECT/StageH - recreating cash_cache/Fields')

df = read_csv('Fields_1.csv')

print df.columns

amt_bb = 2 ## change in the event of a diff db

df['temp'] = df.amt_r_bet_made + df.amt_r_raise_made
df.amt_r_call_won *= df.temp
df.amt_r_call_won *= (df.amt_won + df.amt_bet_ttl)
del df['temp']

df['cnt_pfr'] = df.cnt_p_raise

df['temp'] = df.amt_r_bet_made + df.amt_r_raise_made
df['amt_r_call'] =  df.temp * df.cnt_r_call
df.amt_r_call *= df.amt_bet_r
del df['temp']

df['cnt_f_bet'] = df.flg_f_bet
df['cnt_f_call'] = df.cnt_f_call
df['cnt_f_cbet'] = df.flg_f_cbet

df['cnt_f_cbet_3bet_pot'] = np.logical_and(df.flg_f_cbet == True, np.logical_or(df.flg_p_3bet == True, df.flg_p_4bet == True))

df['cnt_f_cbet_def_action_fold'] = df.enum_f_cbet_action == 'F'
df['cnt_f_cbet_def_action_raise'] = df.enum_f_cbet_action == 'R'

df['cnt_f_cbet_face_raise'] = np.logical_and(df.flg_f_cbet == True, df.flg_f_face_raise == True)

df['cnt_f_cbet_face_raise_3bet_pot'] = np.logical_and(df.flg_f_cbet == True, np.logical_and(df.flg_f_face_raise == True, np.logical_or(df.flg_p_3bet == True, df.flg_p_4bet == True)))

df['cnt_f_cbet_fold_to_raise'] = np.logical_and(df.flg_f_cbet == True, df.action_f == 'BF')

df['cnt_f_cbet_fold_to_raise_3bet_pot'] = np.logical_and(df.flg_f_cbet, np.logical_and(np.logical_or(df.flg_p_3bet, df.flg_p_4bet), df.action_f == 'BF'))

df['cnt_f_cbet_opp'] = df.flg_f_cbet_opp

df['cnt_wtsd'] = df.flg_showdown
df['cnt_vpip'] = df.flg_vpip
df['cnt_t_raise'] = df.cnt_t_raise
df['cnt_t_fold'] = df.flg_t_fold
df['cnt_t_float_opp'] = df.flg_t_float_opp
df['cnt_t_check_raise_opp'] = df.flg_t_check_raise
df['cnt_t_check_raise'] = df.flg_t_check_raise
df['cnt_t_cbet_opp'] = df.flg_t_cbet_opp
df['cnt_t_cbet_def_opp'] = df.flg_t_cbet_def_opp
df['cnt_t_cbet'] = df.flg_t_cbet
df['cnt_t_call'] = df.cnt_t_call
df['cnt_t_bet'] = df.flg_t_bet
df['cnt_r_raise'] = df.cnt_r_raise
df['cnt_r_fold'] = df.flg_r_fold
df['cnt_r_cbet_opp'] = df.flg_r_cbet_opp
df['cnt_r_cbet'] = df.flg_r_bet
df['cnt_r_call'] = df.cnt_r_call
df['cnt_r_bet'] = df.flg_r_bet
df['cnt_pfr'] = df.cnt_p_raise
df['cnt_p_open_opp'] = df.flg_p_open_opp
df['cnt_p_4bet_opp'] = df.flg_p_4bet_opp
df['cnt_p_4bet'] = df.flg_p_4bet
df['cnt_p_3bet_opp'] = df.flg_p_3bet_opp
df['cnt_p_3bet_def_opp'] = df.flg_p_3bet_def_opp
df['cnt_f_saw'] = df.flg_f_saw
df['cnt_f_raise'] = df.cnt_f_raise
df['cnt_f_fold'] = df.flg_f_fold
df['cnt_f_check_raise'] = df.flg_f_check_raise
df['cnt_f_cbet_opp'] = df.flg_f_cbet_opp

df['cnt_f_cbet_opp_3bet_pot'] = np.logical_and(df.flg_f_cbet_opp, np.logical_or(df.flg_p_3bet, df.flg_p_4bet))

df['cnt_f_check_raise_opp'] =np.logical_and(df.flg_f_check, np.logical_or(df.cnt_f_raise > 0, np.logical_or(df.cnt_f_call > 0, df.flg_f_fold)))

df['cnt_f_check_raise_opp'] = np.logical_and(df.flg_f_check, np.logical_or(np.logical_or(df.cnt_f_raise > 0, df.cnt_f_call > 0), df.flg_f_fold))


###################
# UNCHECKED




df['cnt_f_float'] =  np.logical_or(action_p == 'C' , np.logical_and(np.logical_and(np.logical_and(np.logical_and(np.logical_and(action_p == 'CC' , df.flg_p_face_raise ), df.flg_f_bet ), len(str_aggressors_p) == 2 ), str_aggressors_p[1]  > df.position), df.flg_f_has_position))



df['cnt_f_float_opp'] =  np.logical_and(np.logical_and(np.logical_and(np.logical_and(np.logical_and(np.logical_and(np.logical_or(df.action_p == 'C', df.action_p == 'CC') ), df.flg_p_face_raise ), df.flg_f_open_opp ), len(str_aggressors_p) == 2 ), str_aggressors_p[1] > df.position ), df.flg_f_has_position)

df['cnt_f_saw_won'] = np.logical_and(df.flg_won_hand, df.flg_f_saw)
df['cnt_p_3bet'] = df.flg_p_3bet
df['cnt_p_3bet_def_action_call'] = df.enum_p_3bet_action == 'C'
df['cnt_p_3bet_def_df.action_fold_when_open_raised'] = np.logical_and(df.enum_p_3bet_action == 'F', df.flg_p_first_raise)
df['cnt_p_3bet_def_opp_when_open_raised'] = np.logical_and(df.flg_p_3bet_def_opp, df.flg_p_first_raise)
df['cnt_p_3bet_def_opp_when_open_raised_ip'] = np.logical_and(np.logical_and(np.logical_and(np.logical_and(len(str_aggressors_p) >= 3 , df.flg_p_3bet_def_opp ), df.flg_p_first_raise ), str_aggressors_p[2] > df.position))





df['cnt_p_3bet_f_cbet_def_action_raise'] = np.logical_and(np.logical_or(df.flg_p_3bet_def_opp, df.flg_p_4bet_def_opp), df.enum_f_cbet_action == 'R')
df['cnt_p_3bet_f_cbet_def_action_fold'] = np.logical_and(np.logical_or(df.flg_p_3bet_def_opp, df.flg_p_4bet_def_opp), df.enum_f_cbet_action == 'F')

df['cnt_p_3bet_f_cbet_def_opp'] = np.logical_and(np.logical_or(df.flg_p_3bet_def_opp, df.flg_p_4bet_def_opp), df.flg_f_cbet_def_opp)
df['cnt_p_3bet_opp_vs_btn_open'] = np.logical_and(np.logical_and(df.flg_p_3bet_opp , str_aggressors_p[:2] == '80' ), str_actors_p[0] == '0')
df['cnt_p_3bet_opp_vs_co_open'] = df.logical_and(df.logical_and(df.flg_p_3bet_opp, str_aggressors_p[:2] == '81' ), str_actors_p[0] == '1' )


df['cnt_p_3bet_success'] = np.logical_and(np.logical_and(np.logical_and(df.flg_p_3bet , df.flg_won_hand ), df.flg_f_saw == False), df.flg_p_4bet_def_opp == False)

df['cnt_p_3bet_t_cbet_def_action_fold'] = np.logical_and(np.logical_or(df.flg_p_3bet_def_opp, df.flg_p_4bet_def_opp), df.enum_t_cbet_action == 'F')
df['cnt_p_3bet_t_cbet_def_opp'] = np.logical_and(np.logical_or(df.flg_p_3bet_def_opp, df.flg_p_4bet_def_opp), df.flg_t_cbet_def_opp)
df['cnt_p_3bet_vs_btn_open'] = np.logical_and(np.logical_and(df.flg_p_3bet , str_aggressors_p[:2] == '80'), str_actors_p[0] == '0')
df['cnt_p_3bet_vs_co_open'] = np.logical_and(np.logical_and(df.flg_p_3bet , str_aggressors_p[:2]  == '81' ), str_actors_p[0] == '1')
df['cnt_p_4bet_after_raising'] = np.logical_and(df.flg_p_first_raise, df.flg_p_4bet)
df['cnt_p_4bet_def_action_call'] = df.enum_p_4bet_action=='C'
df['cnt_p_4bet_def_action_fold_after_3b'] = np.logical_and(df.flg_p_3bet, df.enum_p_4bet_action == 'F')
df['cnt_p_4bet_def_opp'] = df.flg_p_4bet_def_opp
df['cnt_p_4bet_def_opp_after_3b'] = np.logical_and(df.flg_p_3bet, df.flg_p_4bet_def_opp)
df['cnt_p_4bet_opp_when_open_raised'] = np.logical_and(df.flg_p_4bet_opp, df.flg_p_first_raise)
df['cnt_p_bb_v_sb_3bet'] =  df_logical_and(df_logical_and(df_logical_and(df.position==8 , df.flg_blind_def_opp ), df.action_p[0] == 'R' ), df.val_p_raise_aggressor_pos==9 )
df['cnt_p_bb_v_sb_call'] = df_logical_and(df_logical_and(df_logical_and(df.position==8 , df.flg_blind_def_opp ), df.action_p=='C' ), df.val_p_raise_aggressor_pos==9)
df['cnt_p_bb_v_sb_fold'] = df_logical_and(df_logical_and(df_logical_and(df.position==8 , df.flg_blind_def_opp ), df.action_p=='F' ), df.val_p_raise_aggressor_pos==9)
df['cnt_p_ccall'] = df_logical_and(df.flg_p_ccall, df.amt_p_2bet_facing > 0)
df['cnt_p_ccall_opp'] = df_logical_and(df_logical_and(df.amt_p_2bet_facing > 0 , df.amt_blind == 0 ), df.flg_p_limp == False)
df['cnt_p_raise_3bet'] = df_logical_and(df.enum_p_3bet_action == 'R', df.flg_p_4bet_opp)
df['cnt_p_raise_first_in'] = df_logical_and(df.flg_p_first_raise, df.flg_p_open_opp)
df['cnt_r_call_hands'] = np.logical_and(df.cnt_r_call > 0, df.flg_showdown)
df['cnt_r_cbet_3bet_pot'] = np.logical_and(df.flg_r_cbet, np.logical_or(df.flg_p_3bet, df.flg_p_4bet))
df['cnt_r_cbet_face_raise'] = np.logical_and(df.flg_r_cbet, df.flg_r_face_raise)
df['cnt_r_cbet_face_raise_3bet_pot'] = np.logical_and(np.logical_and(df.flg_r_cbet , df.flg_r_face_raise ), np.logical_or(df.flg_p_3bet, df.flg_p_4bet))
df['cnt_r_cbet_fold_to_raise'] = np.logical_and(df.flg_r_cbet, df.action_r == 'BF')
df['cnt_r_cbet_fold_to_raise_3bet_pot'] = np.logical_and(np.logical_and(df.flg_r_cbet , np.logical_or(df.flg_p_3bet, df.flg_p_4bet)), df.action_r == 'BF')
df['cnt_r_cbet_opp_3bet_pot'] = np.logical_and(df.flg_r_cbet_opp, np.logical_or(df.flg_p_3bet, df.flg_p_4bet))
df['cnt_steal_3bet_def_opp'] = np.logical_and(df.flg_steal_att, df.flg_p_3bet_def_opp)
df['cnt_steal_att_sb'] = np.logical_and(np.logical_and(df.flg_p_open_opp , df.flg_p_first_raise) , df.position == 9)
df['cnt_steal_def_opp_vs_lp'] = np.logical_and(df.flg_blind_def_opp, str_aggressors_p[:2] != '89')
df['cnt_steal_opp_sb'] = np.logical_and(df.flg_p_open_opp, df.position == 9)
df['cnt_t_cbet_3bet_pot'] = np.logical_and(df.flg_t_cbet, np.logical_or(df.flg_p_3bet, df.flg_p_4bet))
df['cnt_t_cbet_def_action_fold'] = df.enum_t_cbet_action=='F'
df['cnt_t_cbet_face_raise'] = np.logical_and(df.flg_t_cbet, df.flg_t_face_raise)

df['cnt_t_cbet_face_raise_3bet_pot'] = df.logical_and(df.logical_and(df.flg_t_cbet , df.flg_t_face_raise ), np.logical_or(df.flg_p_3bet, df.flg_p_4bet))

df['cnt_t_cbet_fold_to_raise'] = np.logical_and(df.flg_t_cbet, df.action_t == 'BF')
df['cnt_t_cbet_fold_to_raise_3bet_pot'] = np.logical_and(np.logical_and(df.flg_t_cbet , np.logical_or(df.flg_p_3bet or df.flg_p_4bet) ), df.action_t == 'BF')
df['cnt_t_cbet_opp_3bet_pot'] = np.logical_and(df.flg_t_cbet_opp, np.logical_or(df.flg_p_3bet, df.flg_p_4bet))
df['cnt_t_float'] = df.flg_t_float
df['cnt_t_float_def_opp'] = df.flg_t_float_def_opp
df['cnt_t_probe'] = np.logical_and(np.logical_and(np.logical_and(np.logical_and(df.flg_t_bet , df.action_f == 'X' ). df.action_p[-1:] != 'R') . df.flg_p_face_raise ), df.val_p_raise_aggressor_pos < df.position)
df['cnt_t_probe_opp'] = np.logical_and(np.logical_and(np.logical_and(np.logical_and(np.logical_and(df.flg_t_open_opp , df.action_f == 'X' ), df.action_p[-1:] != 'R') , df.flg_p_face_raise ), df.val_p_raise_aggressor_pos < df.position) , df.flg_blind_b)
df['cnt_walks'] = df.action_p == ''
df['cnt_wtsd_non_small'] = np.logical_and(df.flg_showdown, df.amt_bet_ttl > 5 * amt_bb)
df['cnt_wtsd_won'] = np.logical_and(df.flg_showdown, df.flg_won_hand)
df['cnt_wtsd_won_non_small'] = np.logical_and(np.logical_and(df.flg_showdown, df.flg_won_hand), df.amt_bet_ttl > 5 * amt_bb)
df['cnt_wtsd_won_when_r_call'] = np.logical_and(np.logical_and(df.cnt_r_call > 0, df.flg_won_hand), df.flg_showdown)


df['cnt_pfr_opp'] =  np.logical_or(len(action_p) >= 2 , np.logical_and(np.logical_and(np.logical_and(len(action_p) ==  1 , df.amt_before > amt_bb ), df.amt_p_raise_facing < df.amt_before - df.amt_blind ), np.logical_or(np.logical_or(np.logical_or(df.flg_p_open_opp, df.cnt_p_face_limpers > 0 ), df.flg_p_3bet_opp ), df.flg_p_4bet_opp)))

df['cnt_f_float_def_opp'] =  np.logical_and(np.logical_and(np.logical_and(np.logical_and(df.flg_p_face_raise == False , action_p[-1:] == 'R' ), df.flg_f_open_opp ), df.flg_f_check ), df.amt_f_bet_facing > 0)

df['cnt_f_float_def_opp_action_fold'] =  np.logical_and(np.logical_and(np.logical_and(np.logical_and(df.flg_p_face_raise == False , action_p[-1:] == 'R' ), df.flg_f_open_opp ), df.amt_f_bet_facing > 0) , action_f[0:2] == 'XF')

df['cnt_p_3bet_def_action_fold_when_open_raised'] = np.logical_and(df.enum_p_3bet_action == 'F', df.flg_p_first_raise)

df['cnt_p_3bet_def_action_fold_when_open_raised_ip'] = np.logical_and(np.logical_and(np.logical_and(len(str_aggressors_p) >= 3 , df.enum_p_3bet_action == 'F' ), df.flg_p_first_raise ), str_aggressors_p[2:3]  > df.position)

df['cnt_p_3bet_def_action_fold_when_open_raised_oop'] = np.logical_and(np.logical_and(np.logical_and(len(str_aggressors_p) >= 3 , df.enum_p_3bet_action == 'F' ), df.flg_p_first_raise ), str_aggressors_p[2:3] < df.position)

df['cnt_steal_3bet_def_action_fold'] = np.logical_and(np.logical_and(df.flg_steal_att , df.flg_p_3bet_def_opp ), df.enum_p_3bet_action == 'F')

df['cnt_steal_def_action_raise_vs_lp'] = np.logical_and(np.logical_and(df.flg_blind_def_opp , action_p[0] == 'R' ), str_aggressors_p[0:2] != '89')

df['cnt_t_float_def_action_fold'] = df.enum_t_float_action == 'F'

df['cnt_p_3bet_def_opp_when_open_raised_oop'] = np.logical_and(np.logical_and(df.flg_p_3bet_def_opp , df.flg_p_first_raise ), df.str_aggressors_p[2] < df.position)



