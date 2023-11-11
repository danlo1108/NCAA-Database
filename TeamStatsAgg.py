#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Wed Oct 17 07:46:31 2018

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


def calculate_team_stats_all(engine):
    team_stats_agg_query='''
    with possessions as (
            select
                tb.team_id
                ,gs.season
                ,.5*(sum(tb.fga) + 0.475*sum(tb.fta) - sum(tb.oreb) + sum(tb.tov)) + .5*(sum(tb.fga_opp) + 0.475*sum(tb.fta_opp) - sum(tb.oreb_opp) + sum(tb.tov_opp)) poss         
            from
                ncaa.team_boxscores tb
            join
                ncaa.game_summaries gs on tb.game_id=gs.game_id and gs.status='Final'
            where season >= 2010
            group by
                tb.team_id
                ,gs.season
            having count(distinct tb.game_id) >= 10
        )
        
    select
        max(case 
            when tb.team_abbr=gs.home_team_abbr then gs.home_team
            when tb.team_abbr=gs.away_team_abbr then gs.away_team else Null end) team
        ,case when tb.team_abbr = 'EKY' then 'EKU' else tb.team_abbr end team_abbr
        ,tb.team_id
        ,gs.season
        ,'All' game_type
        ,count(*) gp
        ,sum(case when tb.pts > tb.pts_opp then 1 else 0 end) wins
        ,sum(case when tb.pts < tb.pts_opp then 1 else 0 end) losses
        ,avg(case when tb.pts > tb.pts_opp then 1.0 else 0 end) win_pct
        ,avg(tb.fgm*1.0) fgm
        ,avg(tb.fga*1.0) fga
        ,sum(tb.fgm)*1.0/sum(tb.fga) fg_pct
        ,avg(tb.fg3m*1.0) fg3m
        ,avg(tb.fg3a*1.0) fg3a
        ,sum(tb.fg3m)*1.0/sum(tb.fg3a) fg3_pct
        ,avg(tb.ftm*1.0) ftm
        ,avg(tb.fta*1.0) fta
        ,sum(tb.ftm)*1.0/sum(tb.fta) ft_pct
        ,avg(tb.pts) pts
        ,avg(tb.oreb)+avg(tb.dreb) reb
        ,avg(tb.oreb) oreb
        ,avg(tb.dreb) dreb
        ,avg(tb.ast) ast
        ,avg(tb.stl) stl
        ,avg(tb.blk) blk
        ,avg(tb.tov) tov
        ,avg(tb.pf) pf
        ,avg(tb.tech_fl) tech_fl
        ,avg(tb.flag_fl) flag_fl
        ,avg(tb.fgm_opp*1.0) fgm_opp
        ,avg(tb.fga_opp*1.0) fga_opp
        ,sum(tb.fgm_opp)*1.0/sum(tb.fga_opp) fg_pct_opp
        ,avg(tb.fg3m_opp*1.0) fg3m_opp
        ,avg(tb.fg3a_opp*1.0) fg3a_opp
        ,sum(tb.fg3m_opp)*1.0/sum(tb.fg3a_opp) fg3_pct_opp
        ,avg(tb.ftm_opp*1.0) ftm_opp
        ,avg(tb.fga_opp*1.0) fta_opp
        ,sum(tb.ftm_opp)*1.0/sum(tb.fta_opp) ft_pct_opp
        ,avg(tb.pts_opp) pts_opp
        ,avg(tb.oreb_opp)+avg(tb.dreb_opp) reb_opp
        ,avg(tb.oreb_opp) oreb_opp
        ,avg(tb.dreb_opp) dreb_opp
        ,avg(tb.ast_opp) ast_opp
        ,avg(tb.stl_opp) stl_opp
        ,avg(tb.blk_opp) blk_opp
        ,avg(tb.tov_opp) tov_opp
        ,avg(tb.pf_opp) pf_opp
        ,avg(tb.tech_fl_opp) tech_fl_opp
        ,avg(tb.flag_Fl_opp) flag_fl_opp
        --Get 4 factors + ratings
        ,sum(pb.mp) mp
        ,p.poss
        ,case when sum(pb.mp) > 0 then 40*((p.poss*2)/(2*(sum(pb.mp)/5.0))) else null end pace
        ,sum(tb.pts)/p.poss*100 off_rtg
        ,sum(tb.pts_opp)*100.0/p.poss def_rtg
        ,(sum(tb.pts)-sum(tb.pts_opp))/p.poss*100 net_rtg
        ,sum(tb.fg3a)*1.0/sum(tb.fga) fg3_rate
        ,sum(tb.fta)*1.0/sum(tb.fga) ft_rate
        ,(sum(tb.fgm)+.5*sum(tb.fg3m))/sum(tb.fga) efg_pct
        ,sum(tb.tov)/(sum(tb.fga)+.475*sum(tb.fta)+sum(tb.tov)) tov_pct
        ,sum(tb.oreb)*1.0/(sum(tb.oreb)+sum(tb.dreb_opp)) oreb_pct
        ,sum(tb.ftm)*1.0/sum(tb.fga) ff_ft_rate
        ,sum(tb.fg3a_opp)*1.0/sum(tb.fga_opp) fg3_rate_opp
        ,sum(tb.fta_opp)*1.0/sum(tb.fga_opp) ft_rate_opp
        ,(sum(tb.fgm_opp)+.5*sum(tb.fg3m_opp))/sum(tb.fga_opp) efg_pct_opp
        ,sum(tb.tov_opp)/(sum(tb.fga_opp)+.475*sum(tb.fta_opp)+sum(tb.tov_opp)) tov_pct_opp
        ,sum(tb.oreb_opp)*1.0/(sum(tb.oreb_opp)+sum(tb.dreb)) oreb_pct_opp
        ,sum(tb.ftm_opp)*1.0/sum(tb.fga_opp) ff_ft_rate_opp
        ,now() last_update_dts
    from
        ncaa.team_boxscores tb
    join
        ncaa.game_summaries gs on tb.game_id=gs.game_id and gs.status='Final'
    left join 
        (select game_id,team_id,sum(cast(mp as float)) mp from ncaa.player_boxscores group by game_id,team_id) pb on tb.game_id=pb.game_id and tb.team_id=pb.team_id
    join
        possessions p on tb.team_id=p.team_id and gs.season=p.season
    group by
        case when tb.team_abbr = 'EKY' then 'EKU' else tb.team_abbr end
        ,tb.team_id
        ,gs.season
        ,p.poss
    having
        sum(tb.fta) > 0
        and sum(tb.fga) > 0
        and sum(tb.fg3a) > 0
    and sum(tb.fta_opp) > 0
    and sum(tb.fga_opp) > 0
    and sum(tb.fg3a_opp) > 0
    and (sum(tb.oreb_opp)+sum(tb.dreb)) > 0
	'''

    team_stats_agg=pd.read_sql(team_stats_agg_query,engine)

    team_stats_agg.to_sql('team_stats_agg',
                          con=engine,
                          schema='ncaa',
                          index=False,
                          if_exists='replace',
                          dtype={'team': sa.types.VARCHAR(255),
                                 'team_abbr': sa.types.CHAR(5), 
                                 'team_id': sa.types.INTEGER(),
                                 'season': sa.types.INTEGER(),
                                 'game_type': sa.types.VARCHAR(255),
                                 'gp': sa.types.INTEGER(),
                                 'wins': sa.types.INTEGER(),
                                 'losses': sa.types.INTEGER(),
                                 'win_pct': sa.types.FLOAT(),
                                 'fgm': sa.types.FLOAT(),
                                 'fga': sa.types.FLOAT(),
                                 'fg_pct': sa.types.FLOAT(),
                                 'fg3m': sa.types.FLOAT(),
                                 'fg3a': sa.types.FLOAT(),
                                 'fg3_pct': sa.types.FLOAT(),
                                 'ftm': sa.types.FLOAT(),
                                 'fta': sa.types.FLOAT(),
                                 'ft_pct': sa.types.FLOAT(),
                                 'pts': sa.types.FLOAT(),
                                 'reb': sa.types.FLOAT(),
                                 'oreb': sa.types.FLOAT(),
                                 'dreb': sa.types.FLOAT(),
                                 'ast': sa.types.FLOAT(),
                                 'stl': sa.types.FLOAT(),
                                 'blk': sa.types.FLOAT(),
                                 'tov': sa.types.FLOAT(),
                                 'pf': sa.types.FLOAT(),
                                 'tech_fl': sa.types.FLOAT(),
                                 'flag_fl': sa.types.FLOAT(),
                                 'fgm_opp': sa.types.FLOAT(), 
                                 'fga_opp': sa.types.FLOAT(),
                                 'fg_pct_opp': sa.types.FLOAT(),
                                 'fg3m_opp': sa.types.FLOAT(),
                                 'fg3a_opp': sa.types.FLOAT(),
                                 'fg3_pct_opp': sa.types.FLOAT(),
                                 'ftm_opp': sa.types.FLOAT(),
                                 'fta_opp': sa.types.FLOAT(),
                                 'ft_pct_opp': sa.types.FLOAT(),
                                 'pts_opp': sa.types.FLOAT(),
                                 'reb_opp': sa.types.FLOAT(),
                                 'oreb_opp': sa.types.FLOAT(),
                                 'dreb_opp': sa.types.FLOAT(),
                                 'ast_opp': sa.types.FLOAT(),
                                 'stl_opp': sa.types.FLOAT(),
                                 'blk_opp': sa.types.FLOAT(),
                                 'tov_opp': sa.types.FLOAT(),
                                 'pf_opp': sa.types.FLOAT(),
                                 'tech_fl_opp': sa.types.FLOAT(),
                                 'flag_fl_opp': sa.types.FLOAT(),
                                 'mp': sa.types.INTEGER(),
                                 'poss': sa.types.FLOAT(),
                                 'pace': sa.types.FLOAT(),
                                 'off_rtg': sa.types.FLOAT(),
                                 'def_rtg': sa.types.FLOAT(),
                                 'net_rtg': sa.types.FLOAT(),
                                 'fg3_rate': sa.types.FLOAT(),
                                 'ft_rate': sa.types.FLOAT(),
                                 'efg_pct': sa.types.FLOAT(),
                                 'tov_pct': sa.types.FLOAT(),
                                 'oreb_pct': sa.types.FLOAT(),
                                 'ff_ft_rate': sa.types.FLOAT(),
                                 'fg3_rate_opp': sa.types.FLOAT(),
                                 'ft_rate_opp': sa.types.FLOAT(),
                                 'efg_pct_opp': sa.types.FLOAT(),
                                 'tov_pct_opp': sa.types.FLOAT(),
                                 'oreb_pct_opp': sa.types.FLOAT(),
                                 'ff_ft_rate_opp': sa.types.FLOAT(),
                                 'last_update_dts': sa.types.DateTime()})
    

def calculate_team_stats_regseason(engine):
    team_stats_agg_query='''
    with possessions as (
            select
                tb.team_id
                ,gs.season
                ,.5*(sum(tb.fga) + 0.475*sum(tb.fta) - sum(tb.oreb) + sum(tb.tov)) + .5*(sum(tb.fga_opp) + 0.475*sum(tb.fta_opp) - sum(tb.oreb_opp) + sum(tb.tov_opp)) poss         
            from
                ncaa.team_boxscores tb
            join
                ncaa.game_summaries gs on tb.game_id=gs.game_id and gs.status='Final' and gs.game_type='Regular Season'
            group by
                tb.team_id
                ,gs.season
        )
        
    select
        max(case 
            when tb.team_abbr=gs.home_team_abbr then gs.home_team
            when tb.team_abbr=gs.away_team_abbr then gs.away_team else Null end) team
        ,case when tb.team_abbr = 'EKY' then 'EKU' else tb.team_abbr end team_abbr
        ,tb.team_id
        ,gs.season
        ,'Regular Season' game_type
        ,count(*) gp
        ,sum(case when tb.pts > tb.pts_opp then 1 else 0 end) wins
        ,sum(case when tb.pts < tb.pts_opp then 1 else 0 end) losses
        ,avg(case when tb.pts > tb.pts_opp then 1.0 else 0 end) win_pct
        ,avg(tb.fgm*1.0) fgm
        ,avg(tb.fga*1.0) fga
        ,sum(tb.fgm)*1.0/sum(tb.fga) fg_pct
        ,avg(tb.fg3m*1.0) fg3m
        ,avg(tb.fg3a*1.0) fg3a
        ,sum(tb.fg3m)*1.0/sum(tb.fg3a) fg3_pct
        ,avg(tb.ftm*1.0) ftm
        ,avg(tb.fta*1.0) fta
        ,sum(tb.ftm)*1.0/sum(tb.fta) ft_pct
        ,avg(tb.pts) pts
        ,avg(tb.oreb)+avg(tb.dreb) reb
        ,avg(tb.oreb) oreb
        ,avg(tb.dreb) dreb
        ,avg(tb.ast) ast
        ,avg(tb.stl) stl
        ,avg(tb.blk) blk
        ,avg(tb.tov) tov
        ,avg(tb.pf) pf
        ,avg(tb.tech_fl) tech_fl
        ,avg(tb.flag_fl) flag_fl
        ,avg(tb.fgm_opp*1.0) fgm_opp
        ,avg(tb.fga_opp*1.0) fga_opp
        ,sum(tb.fgm_opp)*1.0/sum(tb.fga_opp) fg_pct_opp
        ,avg(tb.fg3m_opp*1.0) fg3m_opp
        ,avg(tb.fg3a_opp*1.0) fg3a_opp
        ,sum(tb.fg3m_opp)*1.0/sum(tb.fg3a_opp) fg3_pct_opp
        ,avg(tb.ftm_opp*1.0) ftm_opp
        ,avg(tb.fga_opp*1.0) fta_opp
        ,sum(tb.ftm_opp)*1.0/sum(tb.fta_opp) ft_pct_opp
        ,avg(tb.pts_opp) pts_opp
        ,avg(tb.oreb_opp)+avg(tb.dreb_opp) reb_opp
        ,avg(tb.oreb_opp) oreb_opp
        ,avg(tb.dreb_opp) dreb_opp
        ,avg(tb.ast_opp) ast_opp
        ,avg(tb.stl_opp) stl_opp
        ,avg(tb.blk_opp) blk_opp
        ,avg(tb.tov_opp) tov_opp
        ,avg(tb.pf_opp) pf_opp
        ,avg(tb.tech_fl_opp) tech_fl_opp
        ,avg(tb.flag_Fl_opp) flag_fl_opp
        --Get 4 factors + ratings
        ,sum(pb.mp) mp
        ,p.poss
        ,case when sum(pb.mp) > 0 then 40*((p.poss*2)/(2*(sum(pb.mp)/5.0))) else null end pace
        ,sum(tb.pts)/p.poss*100 off_rtg
        ,sum(tb.pts_opp)*100.0/p.poss def_rtg
        ,(sum(tb.pts)-sum(tb.pts_opp))/p.poss*100 net_rtg
        ,sum(tb.fg3a)*1.0/sum(tb.fga) fg3_rate
        ,sum(tb.fta)*1.0/sum(tb.fga) ft_rate
        ,(sum(tb.fgm)+.5*sum(tb.fg3m))/sum(tb.fga) efg_pct
        ,sum(tb.tov)/(sum(tb.fga)+.475*sum(tb.fta)+sum(tb.tov)) tov_pct
        ,sum(tb.oreb)*1.0/(sum(tb.oreb)+sum(tb.dreb_opp)) oreb_pct
        ,sum(tb.ftm)*1.0/sum(tb.fga) ff_ft_rate
        ,sum(tb.fg3a_opp)*1.0/sum(tb.fga_opp) fg3_rate_opp
        ,sum(tb.fta_opp)*1.0/sum(tb.fga_opp) ft_rate_opp
        ,(sum(tb.fgm_opp)+.5*sum(tb.fg3m_opp))/sum(tb.fga_opp) efg_pct_opp
        ,sum(tb.tov_opp)/(sum(tb.fga_opp)+.475*sum(tb.fta_opp)+sum(tb.tov_opp)) tov_pct_opp
        ,sum(tb.oreb_opp)*1.0/(sum(tb.oreb_opp)+sum(tb.dreb)) oreb_pct_opp
        ,sum(tb.ftm_opp)*1.0/sum(tb.fga_opp) ff_ft_rate_opp
        ,now() last_update_dts
    from
        ncaa.team_boxscores tb
    join
        ncaa.game_summaries gs on tb.game_id=gs.game_id and gs.status='Final' and gs.game_type='Regular Season'
    left join 
        (select game_id,team_id,sum(cast(mp as float)) mp from ncaa.player_boxscores group by game_id,team_id) pb on tb.game_id=pb.game_id and tb.team_id=pb.team_id
    join
        possessions p on tb.team_id=p.team_id and gs.season=p.season
    group by
        case when tb.team_abbr = 'EKY' then 'EKU' else tb.team_abbr end
        ,tb.team_id
        ,gs.season
        ,gs.game_type
        ,p.poss
    having
        sum(tb.fta) > 0
        and sum(tb.fga) > 0
        and sum(tb.fg3a) > 0
    and sum(tb.fta_opp) > 0
    and sum(tb.fga_opp) > 0
    and sum(tb.fg3a_opp) > 0
    and (sum(tb.oreb_opp)+sum(tb.dreb)) > 0
	'''

    team_stats_agg=pd.read_sql(team_stats_agg_query,engine)
    
    team_stats_agg.to_sql('team_stats_agg',
                          con=engine,
                          schema='ncaa',
                          index=False,
                          if_exists='replace',
                          dtype={'team': sa.types.VARCHAR(255),
                                 'team_abbr': sa.types.CHAR(5), 
                                 'team_id': sa.types.INTEGER(),
                                 'season': sa.types.INTEGER(),
                                 'game_type': sa.types.VARCHAR(255),
                                 'gp': sa.types.INTEGER(),
                                 'wins': sa.types.INTEGER(),
                                 'losses': sa.types.INTEGER(),
                                 'win_pct': sa.types.FLOAT(),
                                 'fgm': sa.types.FLOAT(),
                                 'fga': sa.types.FLOAT(),
                                 'fg_pct': sa.types.FLOAT(),
                                 'fg3m': sa.types.FLOAT(),
                                 'fg3a': sa.types.FLOAT(),
                                 'fg3_pct': sa.types.FLOAT(),
                                 'ftm': sa.types.FLOAT(),
                                 'fta': sa.types.FLOAT(),
                                 'ft_pct': sa.types.FLOAT(),
                                 'pts': sa.types.FLOAT(),
                                 'reb': sa.types.FLOAT(),
                                 'oreb': sa.types.FLOAT(),
                                 'dreb': sa.types.FLOAT(),
                                 'ast': sa.types.FLOAT(),
                                 'stl': sa.types.FLOAT(),
                                 'blk': sa.types.FLOAT(),
                                 'tov': sa.types.FLOAT(),
                                 'pf': sa.types.FLOAT(),
                                 'tech_fl': sa.types.FLOAT(),
                                 'flag_fl': sa.types.FLOAT(),
                                 'fgm_opp': sa.types.FLOAT(), 
                                 'fga_opp': sa.types.FLOAT(),
                                 'fg_pct_opp': sa.types.FLOAT(),
                                 'fg3m_opp': sa.types.FLOAT(),
                                 'fg3a_opp': sa.types.FLOAT(),
                                 'fg3_pct_opp': sa.types.FLOAT(),
                                 'ftm_opp': sa.types.FLOAT(),
                                 'fta_opp': sa.types.FLOAT(),
                                 'ft_pct_opp': sa.types.FLOAT(),
                                 'pts_opp': sa.types.FLOAT(),
                                 'reb_opp': sa.types.FLOAT(),
                                 'oreb_opp': sa.types.FLOAT(),
                                 'dreb_opp': sa.types.FLOAT(),
                                 'ast_opp': sa.types.FLOAT(),
                                 'stl_opp': sa.types.FLOAT(),
                                 'blk_opp': sa.types.FLOAT(),
                                 'tov_opp': sa.types.FLOAT(),
                                 'pf_opp': sa.types.FLOAT(),
                                 'tech_fl_opp': sa.types.FLOAT(),
                                 'flag_fl_opp': sa.types.FLOAT(),
                                 'mp': sa.types.INTEGER(),
                                 'poss': sa.types.FLOAT(),
                                 'pace': sa.types.FLOAT(),
                                 'off_rtg': sa.types.FLOAT(),
                                 'def_rtg': sa.types.FLOAT(),
                                 'net_rtg': sa.types.FLOAT(),
                                 'fg3_rate': sa.types.FLOAT(),
                                 'ft_rate': sa.types.FLOAT(),
                                 'efg_pct': sa.types.FLOAT(),
                                 'tov_pct': sa.types.FLOAT(),
                                 'oreb_pct': sa.types.FLOAT(),
                                 'ff_ft_rate': sa.types.FLOAT(),
                                 'fg3_rate_opp': sa.types.FLOAT(),
                                 'ft_rate_opp': sa.types.FLOAT(),
                                 'efg_pct_opp': sa.types.FLOAT(),
                                 'tov_pct_opp': sa.types.FLOAT(),
                                 'oreb_pct_opp': sa.types.FLOAT(),
                                 'ff_ft_rate_opp': sa.types.FLOAT(),
                                 'last_update_dts': sa.types.DateTime()})
    
    
        
    
    
    
def calculate_team_stats_conference(engine):
    team_stats_agg_query='''
    with possessions as (
            select
                tb.team_id
                ,gs.season
                ,.5*(sum(tb.fga) + 0.475*sum(tb.fta) - sum(tb.oreb) + sum(tb.tov)) + .5*(sum(tb.fga_opp) + 0.475*sum(tb.fta_opp) - sum(tb.oreb_opp) + sum(tb.tov_opp)) poss         
            from
                ncaa.team_boxscores tb
            join
                ncaa.game_summaries gs on tb.game_id=gs.game_id and gs.status='Final' and gs.conference_game_flg=True
            group by
                tb.team_id
                ,gs.season
        )
        
    select
        max(case 
            when tb.team_abbr=gs.home_team_abbr then gs.home_team
            when tb.team_abbr=gs.away_team_abbr then gs.away_team else Null end) team
        ,case when tb.team_abbr = 'EKY' then 'EKU' else tb.team_abbr end team_abbr
        ,tb.team_id
        ,gs.season
        ,'Conference' game_type
        ,count(*) gp
        ,sum(case when tb.pts > tb.pts_opp then 1 else 0 end) wins
        ,sum(case when tb.pts < tb.pts_opp then 1 else 0 end) losses
        ,avg(case when tb.pts > tb.pts_opp then 1.0 else 0 end) win_pct
        ,avg(tb.fgm*1.0) fgm
        ,avg(tb.fga*1.0) fga
        ,sum(tb.fgm)*1.0/sum(tb.fga) fg_pct
        ,avg(tb.fg3m*1.0) fg3m
        ,avg(tb.fg3a*1.0) fg3a
        ,sum(tb.fg3m)*1.0/sum(tb.fg3a) fg3_pct
        ,avg(tb.ftm*1.0) ftm
        ,avg(tb.fta*1.0) fta
        ,sum(tb.ftm)*1.0/sum(tb.fta) ft_pct
        ,avg(tb.pts) pts
        ,avg(tb.oreb)+avg(tb.dreb) reb
        ,avg(tb.oreb) oreb
        ,avg(tb.dreb) dreb
        ,avg(tb.ast) ast
        ,avg(tb.stl) stl
        ,avg(tb.blk) blk
        ,avg(tb.tov) tov
        ,avg(tb.pf) pf
        ,avg(tb.tech_fl) tech_fl
        ,avg(tb.flag_fl) flag_fl
        ,avg(tb.fgm_opp*1.0) fgm_opp
        ,avg(tb.fga_opp*1.0) fga_opp
        ,sum(tb.fgm_opp)*1.0/sum(tb.fga_opp) fg_pct_opp
        ,avg(tb.fg3m_opp*1.0) fg3m_opp
        ,avg(tb.fg3a_opp*1.0) fg3a_opp
        ,sum(tb.fg3m_opp)*1.0/sum(tb.fg3a_opp) fg3_pct_opp
        ,avg(tb.ftm_opp*1.0) ftm_opp
        ,avg(tb.fga_opp*1.0) fta_opp
        ,sum(tb.ftm_opp)*1.0/sum(tb.fta_opp) ft_pct_opp
        ,avg(tb.pts_opp) pts_opp
        ,avg(tb.oreb_opp)+avg(tb.dreb_opp) reb_opp
        ,avg(tb.oreb_opp) oreb_opp
        ,avg(tb.dreb_opp) dreb_opp
        ,avg(tb.ast_opp) ast_opp
        ,avg(tb.stl_opp) stl_opp
        ,avg(tb.blk_opp) blk_opp
        ,avg(tb.tov_opp) tov_opp
        ,avg(tb.pf_opp) pf_opp
        ,avg(tb.tech_fl_opp) tech_fl_opp
        ,avg(tb.flag_Fl_opp) flag_fl_opp
        --Get 4 factors + ratings
        ,sum(pb.mp) mp
        ,p.poss
        ,case when sum(pb.mp) > 0 then 40*((p.poss*2)/(2*(sum(pb.mp)/5.0))) else null end pace
        ,sum(tb.pts)/p.poss*100 off_ftg
        ,sum(tb.pts_opp)*100.0/p.poss def_rtg
        ,(sum(tb.pts)-sum(tb.pts_opp))/p.poss*100 net_rtg
        ,sum(tb.fg3a)*1.0/sum(tb.fga) fg3_rate
        ,sum(tb.fta)*1.0/sum(tb.fga) ft_rate
        ,(sum(tb.fgm)+.5*sum(tb.fg3m))/sum(tb.fga) efg_pct
        ,sum(tb.tov)/(sum(tb.fga)+.475*sum(tb.fta)+sum(tb.tov)) tov_pct
        ,sum(tb.oreb)*1.0/(sum(tb.oreb)+sum(tb.dreb_opp)) oreb_pct
        ,sum(tb.ftm)*1.0/sum(tb.fga) ff_ft_rate
        ,sum(tb.fg3a_opp)*1.0/sum(tb.fga_opp) fg3_rate_opp
        ,sum(tb.fta_opp)*1.0/sum(tb.fga_opp) ft_rate_opp
        ,(sum(tb.fgm_opp)+.5*sum(tb.fg3m_opp))/sum(tb.fga_opp) efg_pct_opp
        ,sum(tb.tov_opp)/(sum(tb.fga_opp)+.475*sum(tb.fta_opp)+sum(tb.tov_opp)) tov_pct_opp
        ,sum(tb.oreb_opp)*1.0/(sum(tb.oreb_opp)+sum(tb.dreb)) oreb_pct_opp
        ,sum(tb.ftm_opp)*1.0/sum(tb.fga_opp) ff_ft_rate_opp
        ,now() last_update_dts
    from
        ncaa.team_boxscores tb
    join
        ncaa.game_summaries gs on tb.game_id=gs.game_id and gs.status='Final' and gs.conference_game_flg=True
    left join 
        (select game_id,team_id,sum(cast(mp as float)) mp from ncaa.player_boxscores group by game_id,team_id) pb on tb.game_id=pb.game_id and tb.team_id=pb.team_id
    join
        possessions p on tb.team_id=p.team_id and gs.season=p.season
    group by
        case when tb.team_abbr = 'EKY' then 'EKU' else tb.team_abbr end 
        ,tb.team_id
        ,gs.season
        ,p.poss
    having
        sum(tb.fta) > 0
        and sum(tb.fga) > 0
        and sum(tb.fg3a) > 0
    and sum(tb.fta_opp) > 0
    and sum(tb.fga_opp) > 0
    and sum(tb.fg3a_opp) > 0
    and (sum(tb.oreb_opp)+sum(tb.dreb)) > 0
	'''

    team_stats_agg=pd.read_sql(team_stats_agg_query,engine)

    team_stats_agg.to_sql('team_stats_agg',
                          con=engine,
                          schema='ncaa',
                          index=False,
                          if_exists='replace',
                          dtype={'team': sa.types.VARCHAR(255),
                                 'team_abbr': sa.types.CHAR(5), 
                                 'team_id': sa.types.INTEGER(),
                                 'season': sa.types.INTEGER(),
                                 'game_type': sa.types.VARCHAR(255),
                                 'gp': sa.types.INTEGER(),
                                 'wins': sa.types.INTEGER(),
                                 'losses': sa.types.INTEGER(),
                                 'win_pct': sa.types.FLOAT(),
                                 'fgm': sa.types.FLOAT(),
                                 'fga': sa.types.FLOAT(),
                                 'fg_pct': sa.types.FLOAT(),
                                 'fg3m': sa.types.FLOAT(),
                                 'fg3a': sa.types.FLOAT(),
                                 'fg3_pct': sa.types.FLOAT(),
                                 'ftm': sa.types.FLOAT(),
                                 'fta': sa.types.FLOAT(),
                                 'ft_pct': sa.types.FLOAT(),
                                 'pts': sa.types.FLOAT(),
                                 'reb': sa.types.FLOAT(),
                                 'oreb': sa.types.FLOAT(),
                                 'dreb': sa.types.FLOAT(),
                                 'ast': sa.types.FLOAT(),
                                 'stl': sa.types.FLOAT(),
                                 'blk': sa.types.FLOAT(),
                                 'tov': sa.types.FLOAT(),
                                 'pf': sa.types.FLOAT(),
                                 'tech_fl': sa.types.FLOAT(),
                                 'flag_fl': sa.types.FLOAT(),
                                 'fgm_opp': sa.types.FLOAT(), 
                                 'fga_opp': sa.types.FLOAT(),
                                 'fg_pct_opp': sa.types.FLOAT(),
                                 'fg3m_opp': sa.types.FLOAT(),
                                 'fg3a_opp': sa.types.FLOAT(),
                                 'fg3_pct_opp': sa.types.FLOAT(),
                                 'ftm_opp': sa.types.FLOAT(),
                                 'fta_opp': sa.types.FLOAT(),
                                 'ft_pct_opp': sa.types.FLOAT(),
                                 'pts_opp': sa.types.FLOAT(),
                                 'reb_opp': sa.types.FLOAT(),
                                 'oreb_opp': sa.types.FLOAT(),
                                 'dreb_opp': sa.types.FLOAT(),
                                 'ast_opp': sa.types.FLOAT(),
                                 'stl_opp': sa.types.FLOAT(),
                                 'blk_opp': sa.types.FLOAT(),
                                 'tov_opp': sa.types.FLOAT(),
                                 'pf_opp': sa.types.FLOAT(),
                                 'tech_fl_opp': sa.types.FLOAT(),
                                 'flag_fl_opp': sa.types.FLOAT(),
                                 'mp': sa.types.INTEGER(),
                                 'poss': sa.types.FLOAT(),
                                 'pace': sa.types.FLOAT(),
                                 'off_rtg': sa.types.FLOAT(),
                                 'def_rtg': sa.types.FLOAT(),
                                 'net_rtg': sa.types.FLOAT(),
                                 'fg3_rate': sa.types.FLOAT(),
                                 'ft_rate': sa.types.FLOAT(),
                                 'efg_pct': sa.types.FLOAT(),
                                 'tov_pct': sa.types.FLOAT(),
                                 'oreb_pct': sa.types.FLOAT(),
                                 'ff_ft_rate': sa.types.FLOAT(),
                                 'fg3_rate_opp': sa.types.FLOAT(),
                                 'ft_rate_opp': sa.types.FLOAT(),
                                 'efg_pct_opp': sa.types.FLOAT(),
                                 'tov_pct_opp': sa.types.FLOAT(),
                                 'oreb_pct_opp': sa.types.FLOAT(),
                                 'ff_ft_rate_opp': sa.types.FLOAT(),
                                 'last_update_dts': sa.types.DateTime()})
    
    
def main():
    engine=get_engine()
    calculate_team_stats_all(engine)
    calculate_team_stats_regseason(engine)
    calculate_team_stats_conference(engine)
    
    
if __name__ == "__main__":
    main()
    
    
