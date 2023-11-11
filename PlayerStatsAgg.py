#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Thu Oct 18 20:49:52 2018

@author: dh08loma
"""


import numpy as np
import pandas as pd
import sqlalchemy as sa 
import os
import yaml


def get_engine():
	#Get credentials stored in sql.yaml file (saved in root directory)
	if os.path.isfile('/sql.yaml'):
	    with open("/sql.yaml", 'r') as stream:
	        data_loaded = yaml.load(stream)

	        #domain=data_loaded['SQL_DEV']['domain']
	        user=data_loaded['BBALL_STATS']['user']
	        password=data_loaded['BBALL_STATS']['password']
	        endpoint=data_loaded['BBALL_STATS']['endpoint']
	        port=data_loaded['BBALL_STATS']['port']
	        database=data_loaded['BBALL_STATS']['database']

	db_string = "postgres://{0}:{1}@{2}:{3}/{4}".format(user,password,endpoint,port,database)
	engine=sa.create_engine(db_string)

	return engine


def calculate_player_stats_all(engine):
	team_stats_agg_query='''
	select 
	    pb.player
	    ,pb.player_id
	    ,tsa.team
	    ,pb.team_abbr
        ,pb.team_id
	    ,gs.season
	    ,'All' game_type
	    ,count(*) gp
	    ,sum(pb.mp) mp
	    ,avg(pb.pts) ppg
	    ,avg(pb.fgm) fgm
	    ,avg(pb.fga) fga
	    ,sum(pb.fgm)*1.0/sum(pb.fga) fg_pct
	    ,avg(pb.fg3m) fg3m
	    ,avg(pb.fg3a) fg3a
	    ,case when sum(pb.fg3a) > 0 then sum(pb.fg3m)*1.0/sum(pb.fg3a) else Null end fg3_pct
	    ,avg(pb.ftm) ftm
	    ,avg(pb.fta) fta
	    ,sum(pb.ftm)*1.0/sum(pb.fta) ft_pct
	    ,avg(pb.oreb) oreb
	    ,avg(pb.dreb) dreb
	    ,avg(pb.reb) reb
	    ,avg(pb.ast) ast
	    ,avg(pb.stl) stl
	    ,avg(pb.blk) blk
	    ,avg(pb.tov) tov
	    ,avg(pb.pf) pf
	    ,sum(pb.pts)*40.0/sum(pb.mp) pts_40
	    ,sum(pb.fgm)*40.0/sum(pb.mp) fgm_40
	    ,sum(pb.fga)*40.0/sum(pb.mp) fga_40
	    ,sum(pb.fg3m)*40.0/sum(pb.mp) fg3m_40
	    ,sum(pb.fg3a)*40.0/sum(pb.mp) fg3a_40
	    ,sum(pb.ftm)*40.0/sum(pb.mp) ftm_40
	    ,sum(pb.fta)*40.0/sum(pb.mp) fta_40
	    ,sum(pb.oreb)*40.0/sum(pb.mp) oreb_40
	    ,sum(pb.dreb)*40.0/sum(pb.mp) dreb_40
	    ,sum(pb.reb)*40.0/sum(pb.mp) reb_40
	    ,sum(pb.ast)*40.0/sum(pb.mp) ast_40
	    ,sum(pb.stl)*40.0/sum(pb.mp) stl_40
	    ,sum(pb.blk)*40.0/sum(pb.mp) blk_40
	    ,sum(pb.tov)*40.0/sum(pb.mp) tov_40
	    ,sum(pb.pf)*40.0/sum(pb.mp) PF_40
	    ,(sum(pb.fgm)+sum(pb.fg3m)*.5)/sum(pb.fga) efg_pct
	    ,sum(pb.pts)/(2*(sum(pb.fga)+0.475*sum(pb.fta))) ts_pct
	    ,sum(pb.fg3a)*1.0/sum(pb.fga) fg3_rate
	    ,sum(pb.fta)*1.0/sum(pb.fga) ft_Rate
	    ,(sum(pb.oreb)*(tsa.mp/5.0))/(sum(pb.mp)*(tsa.oreb*tsa.gp+tsa.dreb_opp*tsa.gp)) oreb_pct
	    ,(sum(pb.dreb)*(tsa.mp/5.0))/(sum(pb.mp)*(tsa.dreb*tsa.gp+tsa.oreb_opp*tsa.gp)) dreb_pct
	    ,(sum(pb.reb)*(tsa.mp/5.0))/(sum(pb.mp)*(tsa.reb*tsa.gp+tsa.reb_opp*tsa.gp)) reb_pct
	    ,sum(pb.ast)*1.0/(((sum(pb.mp)/(tsa.mp/5.0))*tsa.fgm*tsa.gp)-sum(pb.fgm)) ast_pct
	    ,sum(pb.stl)*1.0/((sum(pb.mp)/(tsa.mp/5.0))*tsa.poss) stl_pct
	    ,sum(pb.blk)*1.0/((sum(pb.mp)/(tsa.mp/5.0))*(tsa.fga_opp*tsa.gp-tsa.fg3a_opp*tsa.gp)) blk_pct
	    ,sum(pb.tov)*1.0/(sum(pb.fga)+0.475*sum(pb.fta)+sum(pb.tov)) tov_pct
	    ,(sum(pb.fga)+0.475*sum(pb.fta)+sum(pb.tov))/((sum(pb.mp)/(tsa.mp/5.0))*(tsa.fga*tsa.gp+0.475*tsa.fta*tsa.gp+tsa.tov*tsa.gp)) usg_pct    
        ,now() last_update_dts
	from 
	    ncaa.player_boxscores pb 
	join
	    ncaa.game_summaries gs on pb.game_id=gs.game_id
	join
		ncaa.team_stats_agg tsa 
        on pb.team_id=tsa.team_id 
        and gs.season=tsa.season
        and tsa.game_type = 'All'
	where 1=1
	    and pb.dnp_reason is null
	    and gs.season >= 2010
	group by
	    pb.player
	    ,pb.player_id
	    ,tsa.team
	    ,pb.team_abbr
        ,pb.team_id
	    ,gs.season
		,tsa.mp
		,tsa.gp
		,tsa.dreb
		,tsa.oreb_opp
		,tsa.oreb
		,tsa.dreb_opp
		,tsa.reb
		,tsa.reb_opp
		,tsa.fgm
		,tsa.poss
		,tsa.fga_opp
		,tsa.fg3a_opp
		,tsa.fga
		,tsa.fta
		,tsa.tov
	having
	    sum(pb.fga) > 0 and sum(pb.fta) > 0 and sum(pb.tov) > 0 and sum(pb.mp) > 0 
	    and (((sum(pb.mp)/(tsa.mp/5.0))*tsa.fgm*tsa.gp)-sum(pb.fgm)) > 0
	'''

	player_stats_agg=pd.read_sql(team_stats_agg_query,engine)

	player_stats_agg.to_sql('player_stats_agg',
			 con=engine,
                         schema='ncaa',
                         index=False,
                         if_exists='replace',
                         dtype={'player': sa.types.VARCHAR(length=255),
                                'player_id': sa.types.INTEGER(),
                                'team': sa.types.VARCHAR(length=255),
                                'team_abbr': sa.types.CHAR(5),
                                'team_id': sa.types.INTEGER(),
                                'season': sa.types.INTEGER(),
                                'game_type': sa.types.VARCHAR(length=255),
                                'gp': sa.types.INTEGER(),
                                'mp': sa.types.INTEGER(),
                                'ppg': sa.types.FLOAT(),
                                'fgm': sa.types.FLOAT(),
                                'fga': sa.types.FLOAT(),
                                'fg_pct': sa.types.FLOAT(),
                                'fg3m': sa.types.FLOAT(),
                                'fg3a': sa.types.FLOAT(),
                                'fg3_pct': sa.types.FLOAT(),
                                'ftm': sa.types.FLOAT(),
                                'fta': sa.types.FLOAT(),
                                'ft_pct': sa.types.FLOAT(),
                                'oreb': sa.types.FLOAT(),
                                'dreb': sa.types.FLOAT(),
                                'reb': sa.types.FLOAT(),
                                'ast': sa.types.FLOAT(),
                                'stl': sa.types.FLOAT(),
                                'blk': sa.types.FLOAT(),
                                'tov': sa.types.FLOAT(),
                                'pf': sa.types.FLOAT(),
                                'pts_36': sa.types.FLOAT(),
                                'fgm_36': sa.types.FLOAT(),
                                'fga_36': sa.types.FLOAT(),
                                'fg3m_36': sa.types.FLOAT(),
                                'fg3a_36': sa.types.FLOAT(),
                                'ftm_36': sa.types.FLOAT(),
                                'fta_36': sa.types.FLOAT(),
                                'oreb_36': sa.types.FLOAT(),
                                'dreb_36': sa.types.FLOAT(),
                                'reb_36': sa.types.FLOAT(),
                                'ast_36': sa.types.FLOAT(),
                                'stl_36': sa.types.FLOAT(),
                                'blk_36': sa.types.FLOAT(),
                                'tov_36': sa.types.FLOAT(),
                                'pf_36': sa.types.FLOAT(),
                                'efg_pct': sa.types.FLOAT(),
                                'ts_pct': sa.types.FLOAT(),
                                'fg3_rate': sa.types.FLOAT(),
                                'ft_rate': sa.types.FLOAT(),
                                'oreb_pct': sa.types.FLOAT(),
                                'dreb_pct': sa.types.FLOAT(),
                                'reb_pct': sa.types.FLOAT(),
                                'ast_pct': sa.types.FLOAT(),
                                'stl_pct': sa.types.FLOAT(),
                                'blk_pct': sa.types.FLOAT(),
                                'tov_pct': sa.types.FLOAT(),
                                'usg_pct': sa.types.FLOAT(),
                                'last_update_dts': sa.types.DateTime()})


def calculate_player_stats_regseason(engine):
	team_stats_agg_query='''
	select 
	    pb.player
	    ,pb.player_id
	    ,tsa.team
	    ,pb.team_abbr
        ,pb.team_id
	    ,gs.season
	    ,gs.game_type
	    ,count(*) gp
	    ,sum(pb.mp) mp
	    ,avg(pb.pts) ppg
	    ,avg(pb.fgm) fgm
	    ,avg(pb.fga) fga
	    ,sum(pb.fgm)*1.0/sum(pb.fga) fg_pct
	    ,avg(pb.fg3m) fg3m
	    ,avg(pb.fg3a) fg3a
	    ,case when sum(pb.fg3a) > 0 then sum(pb.fg3m)*1.0/sum(pb.fg3a) else Null end fg3_pct
	    ,avg(pb.ftm) ftm
	    ,avg(pb.fta) fta
	    ,sum(pb.ftm)*1.0/sum(pb.fta) ft_pct
	    ,avg(pb.oreb) oreb
	    ,avg(pb.dreb) dreb
	    ,avg(pb.reb) reb
	    ,avg(pb.ast) ast
	    ,avg(pb.stl) stl
	    ,avg(pb.blk) blk
	    ,avg(pb.tov) tov
	    ,avg(pb.pf) pf
	    ,sum(pb.pts)*40.0/sum(pb.mp) pts_40
	    ,sum(pb.fgm)*40.0/sum(pb.mp) fgm_40
	    ,sum(pb.fga)*40.0/sum(pb.mp) fga_40
	    ,sum(pb.fg3m)*40.0/sum(pb.mp) fg3m_40
	    ,sum(pb.fg3a)*40.0/sum(pb.mp) fg3a_40
	    ,sum(pb.ftm)*40.0/sum(pb.mp) ftm_40
	    ,sum(pb.fta)*40.0/sum(pb.mp) fta_40
	    ,sum(pb.oreb)*40.0/sum(pb.mp) oreb_40
	    ,sum(pb.dreb)*40.0/sum(pb.mp) dreb_40
	    ,sum(pb.reb)*40.0/sum(pb.mp) reb_40
	    ,sum(pb.ast)*40.0/sum(pb.mp) ast_40
	    ,sum(pb.stl)*40.0/sum(pb.mp) stl_40
	    ,sum(pb.blk)*40.0/sum(pb.mp) blk_40
	    ,sum(pb.tov)*40.0/sum(pb.mp) tov_40
	    ,sum(pb.pf)*40.0/sum(pb.mp) PF_40
	    ,(sum(pb.fgm)+sum(pb.fg3m)*.5)/sum(pb.fga) efg_pct
	    ,sum(pb.pts)/(2*(sum(pb.fga)+0.475*sum(pb.fta))) ts_pct
	    ,sum(pb.fg3a)*1.0/sum(pb.fga) fg3_rate
	    ,sum(pb.fta)*1.0/sum(pb.fga) ft_Rate
	    ,(sum(pb.oreb)*(tsa.mp/5.0))/(sum(pb.mp)*(tsa.oreb*tsa.gp+tsa.dreb_opp*tsa.gp)) oreb_pct
	    ,(sum(pb.dreb)*(tsa.mp/5.0))/(sum(pb.mp)*(tsa.dreb*tsa.gp+tsa.oreb_opp*tsa.gp)) dreb_pct
	    ,(sum(pb.reb)*(tsa.mp/5.0))/(sum(pb.mp)*(tsa.reb*tsa.gp+tsa.reb_opp*tsa.gp)) reb_pct
	    ,sum(pb.ast)*1.0/(((sum(pb.mp)/(tsa.mp/5.0))*tsa.fgm*tsa.gp)-sum(pb.fgm)) ast_pct
	    ,sum(pb.stl)*1.0/((sum(pb.mp)/(tsa.mp/5.0))*tsa.poss) stl_pct
	    ,sum(pb.blk)*1.0/((sum(pb.mp)/(tsa.mp/5.0))*(tsa.fga_opp*tsa.gp-tsa.fg3a_opp*tsa.gp)) blk_pct
	    ,sum(pb.tov)*1.0/(sum(pb.fga)+0.475*sum(pb.fta)+sum(pb.tov)) tov_pct
	    ,(sum(pb.fga)+0.475*sum(pb.fta)+sum(pb.tov))/((sum(pb.mp)/(tsa.mp/5.0))*(tsa.fga*tsa.gp+0.475*tsa.fta*tsa.gp+tsa.tov*tsa.gp)) usg_pct    
        ,now() last_update_dts
	from 
	    ncaa.player_boxscores pb 
	join
	    ncaa.game_summaries gs on pb.game_id=gs.game_id
	left join
		ncaa.team_stats_agg tsa 
        on pb.team_id=tsa.team_id 
        and gs.season=tsa.season
        and tsa.game_type = 'Regular Season'
    where 1=1
	    and pb.dnp_reason is null
        and gs.game_type='Regular Season'
	group by
	    pb.player
	    ,pb.player_id
	    ,tsa.team
	    ,pb.team_abbr
        ,pb.team_id
	    ,gs.season
		,gs.game_type
		,tsa.mp
		,tsa.gp
		,tsa.dreb
		,tsa.oreb_opp
		,tsa.oreb
		,tsa.dreb_opp
		,tsa.reb
		,tsa.reb_opp
		,tsa.fgm
		,tsa.poss
		,tsa.fga_opp
		,tsa.fg3a_opp
		,tsa.fga
		,tsa.fta
		,tsa.tov
	having
	    sum(pb.fga) > 0 and sum(pb.fta) > 0 and sum(pb.tov) > 0 and sum(pb.mp) > 0 
	    and (((sum(pb.mp)/(tsa.mp/5.0))*tsa.fgm*tsa.gp)-sum(pb.fgm)) > 0
	'''

	player_stats_agg=pd.read_sql(team_stats_agg_query,engine)

	player_stats_agg.to_sql('player_stats_agg',
			 con=engine,
                         schema='ncaa',
                         index=False,
                         if_exists='append',
                         dtype={'player': sa.types.VARCHAR(length=255),
                                'player_id': sa.types.INTEGER(),
                                'team': sa.types.VARCHAR(length=255),
                                'team_abbr': sa.types.CHAR(5),
                                'team_id': sa.types.INTEGER(),
                                'season': sa.types.INTEGER(),
                                'game_type': sa.types.VARCHAR(length=255),
                                'gp': sa.types.INTEGER(),
                                'mp': sa.types.INTEGER(),
                                'ppg': sa.types.FLOAT(),
                                'fgm': sa.types.FLOAT(),
                                'fga': sa.types.FLOAT(),
                                'fg_pct': sa.types.FLOAT(),
                                'fg3m': sa.types.FLOAT(),
                                'fg3a': sa.types.FLOAT(),
                                'fg3_pct': sa.types.FLOAT(),
                                'ftm': sa.types.FLOAT(),
                                'fta': sa.types.FLOAT(),
                                'ft_pct': sa.types.FLOAT(),
                                'oreb': sa.types.FLOAT(),
                                'dreb': sa.types.FLOAT(),
                                'reb': sa.types.FLOAT(),
                                'ast': sa.types.FLOAT(),
                                'stl': sa.types.FLOAT(),
                                'blk': sa.types.FLOAT(),
                                'tov': sa.types.FLOAT(),
                                'pf': sa.types.FLOAT(),
                                'pts_36': sa.types.FLOAT(),
                                'fgm_36': sa.types.FLOAT(),
                                'fga_36': sa.types.FLOAT(),
                                'fg3m_36': sa.types.FLOAT(),
                                'fg3a_36': sa.types.FLOAT(),
                                'ftm_36': sa.types.FLOAT(),
                                'fta_36': sa.types.FLOAT(),
                                'oreb_36': sa.types.FLOAT(),
                                'dreb_36': sa.types.FLOAT(),
                                'reb_36': sa.types.FLOAT(),
                                'ast_36': sa.types.FLOAT(),
                                'stl_36': sa.types.FLOAT(),
                                'blk_36': sa.types.FLOAT(),
                                'tov_36': sa.types.FLOAT(),
                                'pf_36': sa.types.FLOAT(),
                                'efg_pct': sa.types.FLOAT(),
                                'ts_pct': sa.types.FLOAT(),
                                'fg3_rate': sa.types.FLOAT(),
                                'ft_rate': sa.types.FLOAT(),
                                'oreb_pct': sa.types.FLOAT(),
                                'dreb_pct': sa.types.FLOAT(),
                                'reb_pct': sa.types.FLOAT(),
                                'ast_pct': sa.types.FLOAT(),
                                'stl_pct': sa.types.FLOAT(),
                                'blk_pct': sa.types.FLOAT(),
                                'tov_pct': sa.types.FLOAT(),
                                'usg_pct': sa.types.FLOAT(),
                                'last_update_dts': sa.types.DateTime()})
    
    
def calculate_player_stats_conference(engine):
	team_stats_agg_query='''
	select 
	    pb.player
	    ,pb.player_id
	    ,tsa.team
	    ,pb.team_abbr
        ,pb.team_id
	    ,gs.season
	    ,'Conference' game_type
	    ,count(*) gp
	    ,sum(pb.mp) mp
	    ,avg(pb.pts) ppg
	    ,avg(pb.fgm) fgm
	    ,avg(pb.fga) fga
	    ,sum(pb.fgm)*1.0/sum(pb.fga) fg_pct
	    ,avg(pb.fg3m) fg3m
	    ,avg(pb.fg3a) fg3a
	    ,case when sum(pb.fg3a) > 0 then sum(pb.fg3m)*1.0/sum(pb.fg3a) else Null end fg3_pct
	    ,avg(pb.ftm) ftm
	    ,avg(pb.fta) fta
	    ,sum(pb.ftm)*1.0/sum(pb.fta) ft_pct
	    ,avg(pb.oreb) oreb
	    ,avg(pb.dreb) dreb
	    ,avg(pb.reb) reb
	    ,avg(pb.ast) ast
	    ,avg(pb.stl) stl
	    ,avg(pb.blk) blk
	    ,avg(pb.tov) tov
	    ,avg(pb.pf) pf
	    ,sum(pb.pts)*40.0/sum(pb.mp) pts_40
	    ,sum(pb.fgm)*40.0/sum(pb.mp) fgm_40
	    ,sum(pb.fga)*40.0/sum(pb.mp) fga_40
	    ,sum(pb.fg3m)*40.0/sum(pb.mp) fg3m_40
	    ,sum(pb.fg3a)*40.0/sum(pb.mp) fg3a_40
	    ,sum(pb.ftm)*40.0/sum(pb.mp) ftm_40
	    ,sum(pb.fta)*40.0/sum(pb.mp) fta_40
	    ,sum(pb.oreb)*40.0/sum(pb.mp) oreb_40
	    ,sum(pb.dreb)*40.0/sum(pb.mp) dreb_40
	    ,sum(pb.reb)*40.0/sum(pb.mp) reb_40
	    ,sum(pb.ast)*40.0/sum(pb.mp) ast_40
	    ,sum(pb.stl)*40.0/sum(pb.mp) stl_40
	    ,sum(pb.blk)*40.0/sum(pb.mp) blk_40
	    ,sum(pb.tov)*40.0/sum(pb.mp) tov_40
	    ,sum(pb.pf)*40.0/sum(pb.mp) PF_40
	    ,(sum(pb.fgm)+sum(pb.fg3m)*.5)/sum(pb.fga) efg_pct
	    ,sum(pb.pts)/(2*(sum(pb.fga)+0.475*sum(pb.fta))) ts_pct
	    ,sum(pb.fg3a)*1.0/sum(pb.fga) fg3_rate
	    ,sum(pb.fta)*1.0/sum(pb.fga) ft_Rate
	    ,(sum(pb.oreb)*(tsa.mp/5.0))/(sum(pb.mp)*(tsa.oreb*tsa.gp+tsa.dreb_opp*tsa.gp)) oreb_pct
	    ,(sum(pb.dreb)*(tsa.mp/5.0))/(sum(pb.mp)*(tsa.dreb*tsa.gp+tsa.oreb_opp*tsa.gp)) dreb_pct
	    ,(sum(pb.reb)*(tsa.mp/5.0))/(sum(pb.mp)*(tsa.reb*tsa.gp+tsa.reb_opp*tsa.gp)) reb_pct
	    ,sum(pb.ast)*1.0/(((sum(pb.mp)/(tsa.mp/5.0))*tsa.fgm*tsa.gp)-sum(pb.fgm)) ast_pct
	    ,sum(pb.stl)*1.0/((sum(pb.mp)/(tsa.mp/5.0))*tsa.poss) stl_pct
	    ,sum(pb.blk)*1.0/((sum(pb.mp)/(tsa.mp/5.0))*(tsa.fga_opp*tsa.gp-tsa.fg3a_opp*tsa.gp)) blk_pct
	    ,sum(pb.tov)*1.0/(sum(pb.fga)+0.475*sum(pb.fta)+sum(pb.tov)) tov_pct
	    ,(sum(pb.fga)+0.475*sum(pb.fta)+sum(pb.tov))/((sum(pb.mp)/(tsa.mp/5.0))*(tsa.fga*tsa.gp+0.475*tsa.fta*tsa.gp+tsa.tov*tsa.gp)) usg_pct    
        ,now() last_update_dts
	from 
	    ncaa.player_boxscores pb 
	join
	    ncaa.game_summaries gs on pb.game_id=gs.game_id and gs.conference_game_flg=True
	left join
		ncaa.team_stats_agg tsa 
        on pb.team_id=tsa.team_id 
        and gs.season=tsa.season
        and tsa.game_type = 'Conference'
    where 1=1
	    and pb.dnp_reason is null
	group by
	    pb.player
	    ,pb.player_id
	    ,tsa.team
	    ,pb.team_abbr
        ,pb.team_id
	    ,gs.season
		,tsa.mp
		,tsa.gp
		,tsa.dreb
		,tsa.oreb_opp
		,tsa.oreb
		,tsa.dreb_opp
		,tsa.reb
		,tsa.reb_opp
		,tsa.fgm
		,tsa.poss
		,tsa.fga_opp
		,tsa.fg3a_opp
		,tsa.fga
		,tsa.fta
		,tsa.tov
	having
	    sum(pb.fga) > 0 and sum(pb.fta) > 0 and sum(pb.tov) > 0 and sum(pb.mp) > 0 
	    and (((sum(pb.mp)/(tsa.mp/5.0))*tsa.fgm*tsa.gp)-sum(pb.fgm)) > 0
	'''

	player_stats_agg=pd.read_sql(team_stats_agg_query,engine)

	player_stats_agg.to_sql('player_stats_agg',
			 con=engine,
                         schema='ncaa',
                         index=False,
                         if_exists='append',
                         dtype={'player': sa.types.VARCHAR(length=255),
                                'player_id': sa.types.INTEGER(),
                                'team': sa.types.VARCHAR(length=255),
                                'team_abbr': sa.types.CHAR(5),
                                'team_id': sa.types.INTEGER(),
                                'season': sa.types.INTEGER(),
                                'game_type': sa.types.VARCHAR(length=255),
                                'gp': sa.types.INTEGER(),
                                'mp': sa.types.INTEGER(),
                                'ppg': sa.types.FLOAT(),
                                'fgm': sa.types.FLOAT(),
                                'fga': sa.types.FLOAT(),
                                'fg_pct': sa.types.FLOAT(),
                                'fg3m': sa.types.FLOAT(),
                                'fg3a': sa.types.FLOAT(),
                                'fg3_pct': sa.types.FLOAT(),
                                'ftm': sa.types.FLOAT(),
                                'fta': sa.types.FLOAT(),
                                'ft_pct': sa.types.FLOAT(),
                                'oreb': sa.types.FLOAT(),
                                'dreb': sa.types.FLOAT(),
                                'reb': sa.types.FLOAT(),
                                'ast': sa.types.FLOAT(),
                                'stl': sa.types.FLOAT(),
                                'blk': sa.types.FLOAT(),
                                'tov': sa.types.FLOAT(),
                                'pf': sa.types.FLOAT(),
                                'pts_36': sa.types.FLOAT(),
                                'fgm_36': sa.types.FLOAT(),
                                'fga_36': sa.types.FLOAT(),
                                'fg3m_36': sa.types.FLOAT(),
                                'fg3a_36': sa.types.FLOAT(),
                                'ftm_36': sa.types.FLOAT(),
                                'fta_36': sa.types.FLOAT(),
                                'oreb_36': sa.types.FLOAT(),
                                'dreb_36': sa.types.FLOAT(),
                                'reb_36': sa.types.FLOAT(),
                                'ast_36': sa.types.FLOAT(),
                                'stl_36': sa.types.FLOAT(),
                                'blk_36': sa.types.FLOAT(),
                                'tov_36': sa.types.FLOAT(),
                                'pf_36': sa.types.FLOAT(),
                                'efg_pct': sa.types.FLOAT(),
                                'ts_pct': sa.types.FLOAT(),
                                'fg3_rate': sa.types.FLOAT(),
                                'ft_rate': sa.types.FLOAT(),
                                'oreb_pct': sa.types.FLOAT(),
                                'dreb_pct': sa.types.FLOAT(),
                                'reb_pct': sa.types.FLOAT(),
                                'ast_pct': sa.types.FLOAT(),
                                'stl_pct': sa.types.FLOAT(),
                                'blk_pct': sa.types.FLOAT(),
                                'tov_pct': sa.types.FLOAT(),
                                'usg_pct': sa.types.FLOAT(),
                                'last_update_dts': sa.types.DateTime()})

def main():
    engine=get_engine()
    calculate_player_stats_all(engine)
    calculate_player_stats_regseason(engine)
    calculate_player_stats_conference(engine)
    
    
if __name__ == "__main__":
    main()



