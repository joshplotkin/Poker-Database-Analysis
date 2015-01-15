import pandas as pd
from pandas import *
import os

os.chdir('X:/Users/Josh/Desktop/POKER PROJECT/StageF - Dump Tables/cash_hand_player_statistics')

def import_csv(db, batch):
    path = 'X:/Users/Josh/Desktop/POKER PROJECT/StageF - Dump Tables/cash_hand_player_statistics/'
    path += str(db) + '/16_batch' + str(batch) + '_' + str(db) + '.csv'
    return read_csv(path)

def get_valid_hand(db):
    path = 'X:/Users/Josh/Desktop/POKER PROJECT/StageF - Dump Tables/Tables/'
    path += '15_cash_limit_' + str(db) + '.csv'
    df = read_csv(path, index_col = 'id_limit')

    df['valid'] = True
    df.limit_currency = df.limit_currency.apply(lambda x: 0 if x != 'USD' else 1)
    df.amt_bb = df.amt_bb.apply(lambda x: 0 if x != 2 else 1)
    df.flg_nl = df.flg_nl.apply(lambda x: 0 if x == False else 1)
    df.flg_sh = df.flg_sh.apply(lambda x: 0 if x == False else 1)
    df['valid'] = df.limit_currency + df.amt_bb + df.flg_nl + df.flg_sh

    df.valid = df.valid.apply(lambda x: True if x == 4 else False)

    return df[['valid','limit_name','amt_bb']]

def get_id_hand(df):
    hand_num_list = []
    counter = 0
    hand_num = 0
    df = df[['cnt_players']].values.tolist()
    for row in range(len(df)):
        if counter == 0:
            hand_num += 1
            counter = df[row][0]

        hand_num_list.append(hand_num)
        counter -= 1

    return hand_num_list

def action(db):
    path = 'X:/Users/Josh/Desktop/POKER PROJECT/StageF - Dump Tables/Tables/'
    path += '15_lookup_actions_' + str(db) + '.csv'
    df = read_csv(path)
    return df

def action_street_df(df, street):
    df['action_' + street] = df.action
    df['id_action_' + street] = df.id_action
    df = df[['action_' + street,'id_action_' + street]]
    df.set_index('id_action_' + street, inplace = True)
    return df

def hand_no(db):
    hn = read_csv('X:/Users/Josh/Desktop/POKER PROJECT/StageF - Dump Tables/Tables/15_cash_hand_summary_'
        + str(db) + '.csv')
    return hn[['id_hand','hand_no','card_1','card_2','card_3','card_4','card_5']]

def get_names(db):
    path = 'X:/Users/Josh/Desktop/POKER PROJECT/StageF - Dump Tables/Tables/'
    path += '15_player_' + str(db) + '.csv'
    df = read_csv((path), index_col = 'id_player')

    fix_name = lambda x: unicode(x, errors = 'replace').replace(u"\ufffd",'*')
    df.player_name = df.player_name.apply(fix_name)
    return df[['player_name']]

def get_positions(db):
    pos = read_csv('X:/Users/Josh/Desktop/POKER PROJECT/StageF - Dump Tables/Tables/15_lookup_positions_' + str(db) + '.csv', index_col = ('cnt_players','position'))

    return pos [['description']]

def clean_db(db, batchrange):

    for batch in batchrange:
        df = import_csv(db, batch) # import the appropriate csv

        if 'Unnamed: 0' in df.columns: # get rid of unwanted Unnamed: 0 column
            del df['Unnamed: 0']

        if batch == 1:
            if 'id_hand' in df.columns:
                id_list = df.id_hand.values.tolist()

            else:
                # create id_hand
                id_list = get_id_hand(df) # return hand_num_list
                df['id_hand'] = id_list

            # filter for valid hands
            valid_limit = get_valid_hand(db)
            df = df.merge(valid_limit, left_on = 'id_limit', right_index = True, how = 'left')

            valid_list = df.valid.values.tolist()

            df = df[df.valid == True]
            del df['valid']
            # cleaned up scrennames
            names = get_names(db)
            df = df.join(names, on = 'id_player', how = 'left')

            # get positions
            pos_desc = get_positions(db)
            df = df.join(pos_desc, on = ('cnt_players','position'), how = 'left')

            endpoint = np.max(id_list)

            summary_df = df[['id_hand','player_name','amt_bb','description']] # a summary df of the db
            summary_df.set_index(['id_hand','player_name'], inplace = True)
            summary_df.index.name = len(summary_df)
            summary_df.to_csv('X:/Users/Josh/Desktop/POKER PROJECT/StageG/db' + str(db) + '_SUMMARY.csv')

        else:

            if 'id_hand' not in df.columns:
                # create id_hand
                df['id_hand'] = id_list

            if batch == 3: ## get hand_number and board cards with id_hand
                handnum = hand_no(db)
                df = df.merge(handnum, on = 'id_hand', how = 'left')

            ## keep a running count of hand_id to make it unique
            ## across all db
            df.id_hand += endpoint + 1
            endpoint = df.id_hand.max()

            if 'id_limit' not in df.columns:
                df['valid'] = valid_list
            else:
                df = df.merge(valid_limit, left_on = 'id_limit', right_index = True, how = 'left')
            df = df[df.valid == True]
            del df['valid']

            if 'id_limit' in df.columns:
                del df['id_limit']

            # player names (from batch 1)
            df = df.join(names, on = 'id_player', how = 'left')
            del df['id_player']

        df.set_index(['id_hand','player_name'], inplace = True)


        ### for batch9, action summaries
        if batch == 9:
            action_df = action(db)
            for street in ('p','f','t','r'):
                # df of actions with suffixes for this street
                street_df  = action_street_df(action_df,street)

                # names of columns
                id_action_street = 'id_action_' + street
                action_street = 'action_' + street
                # get the id_action_str into the summary df
                # summary_df[id_action_street] = df[id_action_street].copy()

                # summary_df = summary_df.join(df[id_action_street], how = 'left')

                df = df.join(street_df, on = id_action_street, how = 'left')

                ### add actions to summary_df (stopped doing this)
                # summary_df = summary_df.join(street_df, on = id_action_street, how = 'left')
                # del summary_df[id_action_street]

        df.to_csv('X:/Users/Josh/Desktop/POKER PROJECT/StageG/db' + str(db) + '_batch' + str(batch) + '.csv')

endpoint = 0 # for global running count of hand_id
for db in range(1,2): # what else do i want on summary df? 3. amt_expected_won, 9. amt_before
# for db in range(1,2):
    clean_db(db, [i for i in range(1,13)])# if i in (1, 11, 12)]) # loop
    # clean_db(db, [1,10,11,12]) # loop
    # clean_db(db, [1,9]) # loop
    # clean_df.head()







'''
sum(if[ lookup_actions_p.action LIKE '__%'
    OR (lookup_actions_p.action LIKE '_'
        AND (cash_hand_player_statistics.amt_before > (cash_limit.amt_bb + cash_hand_player_statistics.amt_ante))
        AND (cash_hand_player_statistics.amt_p_raise_facing < (cash_hand_player_statistics.amt_before - (cash_hand_player_statistics.amt_blind + cash_hand_player_statistics.amt_ante)))
        AND (cash_hand_player_statistics.flg_p_open_opp
            OR cash_hand_player_statistics.cnt_p_face_limpers > 0
            OR cash_hand_player_statistics.flg_p_3bet_opp
            OR cash_hand_player_statistics.flg_p_4bet_opp) ) 1, 0])
'''
