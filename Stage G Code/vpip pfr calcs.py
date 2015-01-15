(cnt_vpip / (cnt_hands - cnt_walks)) * 100


walks:
sum(if[lookup_actions_p.action = '', 1, 0])
db 9

flg_vpip
db 1

action_p (9)
flg_vpip (9)
# hands

cnt_p_raise (1)
(1) has money left, AND (2) not facing all-in, AND (3) could (a) open (b) face limpers (c) 3-bet (d) 4-bet

 action_p (9)
 amt_before (9)
 amt_bb
amt_p_raise_facing (10)
amt_blind (9)
flg_p_open_opp (9)
cnt_p_face_limpers > 0  (1)
flg_p_3bet_opp (4)
flg_p_4bet_opp (4)

sum(
        if[
        # it started with Raise,
        lookup_actions_p.action LIKE '__%'
        OR
        (
            # it started with fold
            lookup_actions_p.action LIKE '_'

            AND
            (
                        # player started the hand with enough money to play
                        cash_hand_player_statistics.amt_before >
                        (cash_limit.amt_bb + cash_hand_player_statistics.amt_ante)
            )

            AND
            (

                # not facing an all-in
                cash_hand_player_statistics.amt_p_raise_facing <
                (
                    # amount after posting blind/ante
                    cash_hand_player_statistics.amt_before -
                    (cash_hand_player_statistics.amt_blind + cash_hand_player_statistics.amt_ante)
                )
            )

            AND
            (
                # could open preflop OR facing limpers OR could 3-bet OR could 4-bet
                cash_hand_player_statistics.flg_p_open_opp
                OR cash_hand_player_statistics.cnt_p_face_limpers > 0
                OR cash_hand_player_statistics.flg_p_3bet_opp
                OR cash_hand_player_statistics.flg_p_4bet_opp
            )

        )
, 1, 0]
)
