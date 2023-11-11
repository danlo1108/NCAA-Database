# -*- coding: utf-8 -*-
"""
Created on Sat Mar  3 16:48:35 2018

@author: DanLo1108
"""

from bs4 import BeautifulSoup
import numpy as np
import pandas as pd
from urllib.request import urlopen
import re
import os
import yaml
import sqlalchemy as sa

#Function which takes a date string and appends game summaries
#to PostGres database
def append_game_summary(date_str,engine):
    
    #Define URL from ESPN
    #url='https://www.espn.com/mens-college-basketball/scoreboard/_/date/'+date_str+'/group/'+group_num
    url='https://www.espn.com/mens-college-basketball/scoreboard/_/date/'+date_str+'/group/50'
    
    #Get URL page 
    page = urlopen(url)
    
    #Get content from URL page
    content=page.read()
    soup=BeautifulSoup(content,'lxml')
    
    #Get scripts
    scripts=soup.find_all('script')
    
    #Get results from scripts
    results=[script.contents[0] for script in scripts if len(script.contents) > 0 and '{"league"' in script.contents[0]][0]
    results=results[results.index('evts":[')+6:results.index('hideScoreDate":true}]')+21]
    results=re.sub('false','False',results)
    results=re.sub('true','True',results)
    results=re.sub('null','"Null"',results)
    
    events=eval(results)#['evts']
    
    #Iterate through "events" i.e. games
    scoreboard_results=[]
    for event in events:
        game_id=event['id'] #Game ID
        date=date_str[4:6]+'-'+date_str[6:]+'-'+date_str[:4] #Date
        
        if int(date_str[4:6]) < 10:
            season=int(date_str[:4])
        else:
            season=int(date_str[:4])+1
        
        #Get venue/attendance
        if 'vnue' in event:
            venue=event['vnue']['fullName']
            if 'address' in event['vnue']:
                if 'state' in event['vnue']['address'] and 'city' in event['vnue']['address']:
                    location=event['vnue']['address']['city']+', '+event['vnue']['address']['state']
                else:
                    location=None
            else:
                location=None
            venue_id=event['vnue']['id']
        else:
            venue=None
            location=None
            venue_id=None

        attendance=None
        neutral_site_flg=None

        game_type='Regular Season'
        postseason_tourney=None
        #Get game type (preseason/reg season/postseason)
        if 'type' in event['watchListen']['cmpttn']['lg']['season']:
            if event['watchListen']['cmpttn']['lg']['season']['type']['type']==1:
                game_type='Preseason'
            elif event['watchListen']['cmpttn']['lg']['season']['type']['type']==2:
                game_type='Regular Season'
            elif event['watchListen']['cmpttn']['lg']['season']['type']['type']==3:
                game_type='Postseason'
        else:
            game_type=None
        
        #Get long and short headlines for game
        # if 'headlines' in event['highlights']:
        #     headline_long=None
        #     headline_short=event['highlights']['headline']
        # else:
        headline_long=None
        headline_short=None
            
        if 'note' in event:#'headline' in event['competitions'][0]['notes'][0]:
            notes=event['note']
        else:
            notes=None
            
        if 'broadcasts' in event:
            try:
                broadcast=event['broadcasts'][0]['name']
            except:
                broadcast=None
        else:
            broadcast=None

        
        for competitor in event['competitors']:
            
            if competitor['isHome']:

                home_team_id=competitor['id']
                home_team_abbr=competitor['abbrev']
                home_team=competitor['displayName']

                if 'score' in competitor:
                    home_team_score=competitor['score']
                else:
                    home_team_score = None

                if 'logo' in competitor:
                    home_team_d1_flg=1
                else:
                    home_team_d1_flg=0

                if 'winner' in competitor:
                    home_team_winner=True
                else:
                    home_team_winner=False

                home_team_conference_id=None
                home_team_overall_record = None
                home_team_conference_record = None 


                home_team_home_record=None
                home_team_away_record=None
            
                if 'rank' in competitor:
                    home_team_rank_seed = competitor['rank']
                else:
                    home_team_rank_seed = np.nan

            else:

                try:
                    away_team_id=competitor['id']
                    away_team_abbr=competitor['abbrev']
                    away_team=competitor['displayName']
                except:
                    away_team_id=None
                    away_team_abbr=None
                    away_team=None
                    

                if 'score' in competitor:
                    away_team_score=competitor['score']
                else:
                    away_team_score = None

                if 'logo' in competitor:
                    away_team_d1_flg=1
                else:
                    away_team_d1_flg=0

                if 'winner' in competitor:
                    away_team_winner=True
                else:
                    away_team_winner=False

                away_team_conference_id=None
                away_team_overall_record=None
                away_team_conference_record=None
                
                away_team_home_record=None
                away_team_away_record=None

                if 'rank' in competitor:
                    away_team_rank_seed = competitor['rank']
                else:
                    away_team_rank_seed = np.nan

        if 'cnfrnce' in event:
            group_conference_flg=None
            conference_game_flg=1
            group_id=None
            group_name = event['cnfrnce']
        else:
            group_conference_flg=None
            conference_game_flg=0
            group_id=None
            group_name=None


        status = event['status']['description']
        if 'detail' in event['status']:
            ot_status = event['status']['detail']
        else:
            ot_status = None

        if home_team_rank_seed == 99:
            home_team_rank_seed=np.nan
        
        if away_team_rank_seed == 99:
            away_team_rank_seed=np.nan

        if game_type=='Postseason':
            if 'region' in notes.lower():
                postseason_tourney = 'NCAA'
                ncaa_tournament_flg=1
            elif 'nit' in notes.lower():
                postseason_tourney = 'NIT'
                ncaa_tournament_flg=0
            elif 'cit' in notes.lower():
                postseason_tourney = 'CIT'
                ncaa_tournament_flg=0
            elif 'cbi' in notes.lower():
                postseason_tourney = 'NCAA'
                ncaa_tournament_flg=0
            if 'the basketball classic' in notes.lower():
                postseason_tourney = 'The Basketball Classic'
                ncaa_tournament_flg=0
        else:
            postseason_tourney = None
            ncaa_tournament_flg=0
            
        #Append game results to list   
        scoreboard_results.append((game_id,status,ot_status,game_type,neutral_site_flg,date,season,
                                  home_team,away_team,home_team_rank_seed,away_team_rank_seed,home_team_score,
                                  away_team_score,location,venue,venue_id,attendance,broadcast,
                                  headline_long,headline_short,home_team_abbr,home_team_id,home_team_conference_id,home_team_d1_flg,
                                  home_team_winner,away_team_abbr,
                                  away_team_id,away_team_conference_id,away_team_d1_flg,away_team_winner,conference_game_flg,notes,
                                  group_conference_flg,group_id,group_name,
                                  home_team_overall_record,home_team_conference_record,home_team_home_record,home_team_away_record,
                                  away_team_overall_record,away_team_conference_record,away_team_home_record,away_team_away_record,
                                  postseason_tourney,ncaa_tournament_flg))
    
    #Define column names
    col_names=['game_id','status','status_detail','game_type','neutral_site_flg',
               'date','season','home_team','away_team','home_team_rank_seed','away_team_rank_seed',
               'home_team_score','away_team_score','location','venue','venue_id','attendance',
               'broadcast','headline_long','headline_short','home_team_abbr','home_team_id',
               'home_team_conference_id','home_team_d1_flg','home_team_winner','away_team_abbr',
               'away_team_id','away_team_conference_id','away_team_d1_flg','away_team_winner',
               'conference_game_flg','notes','group_conference_flg','group_id','group_name',
               'home_team_overall_record','home_team_conference_record','home_team_home_record',
               'home_team_away_record','away_team_overall_record','away_team_conference_record',
               'away_team_home_record','away_team_away_record','postseason_tourney','ncaa_tournament_flg']  
     
    #Save all games for date to DF                           
    scoreboard_results_df=pd.DataFrame(scoreboard_results,columns=col_names)
    
    #Append dataframe results to PostGres database
    scoreboard_results_df.to_sql('game_summaries',
                                 con=engine,schema='ncaa',
                                 index=False,
                                 if_exists='append',
                                 dtype={'game_id': sa.types.INTEGER(),
                                        'status': sa.types.VARCHAR(length=255),
                                        'status_detail': sa.types.VARCHAR(length=255),
                                        'game_type': sa.types.VARCHAR(length=255),
                                        'neutral_site_flg': sa.types.BOOLEAN(), 
                                        'date': sa.types.Date(),
                                        'season': sa.types.INTEGER(),
                                        'home_team': sa.types.VARCHAR(length=255),
                                        'away_team': sa.types.VARCHAR(length=255),
                                        'home_team_rank_seed': sa.types.INTEGER(), 
                                        'away_team_rank_seed': sa.types.INTEGER(),
                                        'home_team_score': sa.types.INTEGER(),
                                        'away_team_score': sa.types.INTEGER(),
                                        'location': sa.types.VARCHAR(length=255),
                                        'venue': sa.types.VARCHAR(length=255),
                                        'venue_id': sa.types.INTEGER(),
                                        'attendance': sa.types.INTEGER(),
                                        'broadcast': sa.types.VARCHAR(length=255),
                                        'headline_long': sa.types.VARCHAR(length=255),
                                        'headline_short': sa.types.VARCHAR(length=255),
                                        'home_team_abbr': sa.types.VARCHAR(length=255),
                                        'home_team_id': sa.types.INTEGER(),
                                        'home_team_conference_id': sa.types.INTEGER(),
                                        'home_team_d1_flg': sa.types.BOOLEAN(), 
                                        'home_team_winner': sa.types.BOOLEAN(),
                                        'away_team_abbr': sa.types.VARCHAR(length=255),
                                        'away_team_id': sa.types.INTEGER(),
                                        'away_team_conference_id': sa.types.INTEGER(),
                                        'away_team_d1_flg': sa.types.BOOLEAN(), 
                                        'away_team_winner': sa.types.BOOLEAN(),
                                        'conference_game_flg': sa.types.BOOLEAN(),
                                        'notes': sa.types.VARCHAR(length=255),
                                        'group_conference_flg': sa.types.BOOLEAN(),
                                        'group_id': sa.types.INTEGER(),
                                        'group_name': sa.types.VARCHAR(length=255),
                                        'home_team_overall_record': sa.types.VARCHAR(length=255),
                                        'home_team_conference_record': sa.types.VARCHAR(length=255),
                                        'home_team_home_record': sa.types.VARCHAR(length=255),
                                        'home_team_away_record': sa.types.VARCHAR(length=255),
                                        'away_team_overall_record': sa.types.VARCHAR(length=255),
                                        'away_team_conference_record': sa.types.VARCHAR(length=255),
                                        'away_team_home_record': sa.types.VARCHAR(length=255),
                                        'away_team_away_record': sa.types.VARCHAR(length=255),
                                        'postseason_tourney': sa.types.VARCHAR(length=255),
                                        'ncaa_tournament_flg': sa.types.BOOLEAN()}
                                 )
    

#Get credentials stored in sql.yaml file (saved in root directory)
def get_engine():

	#Yaml stored in directory above script directory (where repository was cloned)
	fp=os.path.dirname(os.path.realpath(__file__))
	yaml_fp=fp[:fp.index('NCAA-Database')]

	if os.path.isfile(yaml_fp+'sql.yaml'):
		with open(yaml_fp+'sql.yaml', 'r') as stream:
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


#Get max dates of games that were scheduled but not completed
import datetime
from datetime import date
from datetime import timedelta

def get_dates(engine):
    start_date = datetime.date.today() - timedelta(days=1)
    end_date = datetime.date.today() - timedelta(days=1)
    
    dates = pd.date_range(start=start_date, end=end_date, freq='D')
    formatted_dates = dates.strftime('%Y%m%d').tolist()
    
    return formatted_dates


def update_game_summaries(engine,dates): 
    #Iterate through list of dates, appending each days games
    for date_str in dates: 
        try:
            append_game_summary(date_str,engine)
        except:
            print(date_str)
    
 
def drop_sched_rows(engine):
    #Drop old rows from games that were scheduled and now completed or has new metadata
    drop_old_rows_query='''

    delete from
        ncaa.game_summaries gs
    where
        status = 'Scheduled'
        and date < (now() - interval '1 day')

    '''

    engine.execute(drop_old_rows_query)



def main():
    engine=get_engine()
    dates_list=get_dates(engine)
    drop_sched_rows(engine)
    update_game_summaries(engine,dates_list)
    
    
    
if __name__ == "__main__":
    main() 




