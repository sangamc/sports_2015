__author__ = 'tanyacashorali'

import jsonpickle
import random
import urllib2
import time
import re
import random
from datetime import date, timedelta
import datetime
import os
import sqlite3
import pandas as pd
from urlparse import urlparse
from bs4 import BeautifulSoup as bs

db = sqlite3.connect('/home/ec2-user/sports2015/NCAA/sports.db')

x=random.randint(4, 10)
time.sleep(x)

cur = db.execute("SELECT game_id from NCAAstats")
game_ids = cur.fetchall()
game_ids = [str(g[0]) for g in game_ids]

def index():
    print "entered index"
    times = []
    halftime_ids = []
    today = date.today()
    today = today.strftime("%Y%m%d")
    #vals = [50,3,46,2,1,62,8,4,5,6,7,9,11,10,45,12,13,14,16,18,44,19,20,21,22,23,26,24,25,49,27,30,29]
    vals = [50]
    for v in range(0,len(vals)):
        url = urllib2.urlopen('http://scores.espn.go.com/mens-college-basketball/scoreboard/_/group/' + str(vals[v]) + '/date/' + today)
        soup = bs(url.read(), ['fast', 'lxml'])
        data=re.search('window.espn.scoreboardData.*{(.*)};</script>', str(soup)).group(0)
        jsondata=re.search('({.*});window', data).group(1)
        j=jsonpickle.decode(jsondata)
        games=j['events']
        status = [game['status'] for game in games]
        half = [s['type']['shortDetail'] for s in status]
        index = [i for i, j in enumerate(half) if j == 'Halftime']
        ids = [game['id'] for game in games]
        halftime_ids = [j for k, j in enumerate(ids) if k in index]
        halftime_ids = list(set(halftime_ids) - set(game_ids))
        print len(halftime_ids)
        league = 'ncaa'
        if(len(halftime_ids) == 0):
            print "No Halftime Box Scores yet."
        else:
            for i in range(0, len(halftime_ids)):
                x=random.randint(2, 5)
                time.sleep(x)
                espn = 'http://espn.go.com/mens-college-basketball/boxscore?gameId=' + halftime_ids[i]
                url = urllib2.urlopen(espn)
                soup = bs(url.read(), ['fast', 'lxml'])
                game_date = soup.findAll('div', {'class':'game-time-location'})[0].p.text
                the_date =  re.search(',\s(.*)', game_date).group(1)
                the_time = re.search('^(.*?),', game_date)
                game_time = the_time.group(1)
                t=time.strptime(the_date, "%B %d, %Y")
                gdate=time.strftime('%m/%d/%Y', t)
                boxscore = soup.find('table', {'class':'mod-data'})
                try: 
                    theads=boxscore.findAll('thead')
                    the_thead = None
                    for thead in theads:
                        ths=thead.findAll('th')
                        labels = [x.text for x in ths]
                        if (any("TOTALS" in s for s in labels)):
                            the_thead = thead
                            break
                    headers = the_thead.findAll('th')
                    header_vals = [h.text for h in headers]
                    team1_data = the_thead.nextSibling
                    tds = team1_data.findAll('td')
                    values = [v.text for v in tds]
                    remove = [v == '' for v in values]
                    remove_indices = [a for a, b in enumerate(remove) if b]
                    cleaned1 = [j for k, j in enumerate(values) if k not in remove_indices]
                    linescore = soup.find('table', {'class':'linescore'})
                    team1 = linescore.findAll('a')[0].text
                    team2 = linescore.findAll('a')[1].text
                    if team1 == 'WM':
                        team1 = 'W&M'
                    if team2 == 'WM':
                        team2 = 'W&M'
                    if team1 == 'TAM':
                        team1 = 'TA&M'
                    if team2 == 'TAM':
                        team2 = 'TA&M'
                    team1_data = the_thead.nextSibling.nextSibling.nextSibling.nextSibling.nextSibling.nextSibling
                    tds = team1_data.findAll('td')
                    values = [v.text for v in tds]
                    remove = [v == '' for v in values]
                    remove_indices = [j for j, x in enumerate(remove) if x]
                    cleaned2 = [j for k, j in enumerate(values) if k not in remove_indices]
                    cleaned1.insert(0, team1)
                    cleaned2.insert(0, team2)
                    cleaned1.insert(0, halftime_ids[i])
                    cleaned2.insert(0, halftime_ids[i])
                    cleaned1.pop()
                    cleaned1.pop()
                    cleaned1.pop()
                    cleaned2.pop()
                    cleaned2.pop()
                    cleaned2.pop()
                    try:
                        with db:
                            db.execute('''INSERT INTO NCAAgames(game_id, team1, team2, game_date, game_time) VALUES(?,?,?,?,?)''', (halftime_ids[i], team1, team2, gdate, game_time))
                            db.commit()
                        try:
                            date_time = str(datetime.datetime.now())
                            db.execute('''INSERT INTO NCAAstats(game_id, team, fgma, tpma, ftma, oreb, dreb, reb, ast, stl, blk, turnovers, pf, pts, timestamp ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''', (halftime_ids[i], team1, cleaned1[2], cleaned1[3], cleaned1[4], int(cleaned1[5]), int(cleaned1[6]), int(cleaned1[7]), int(cleaned1[8]), int(cleaned1[9]), int(cleaned1[10]), int(cleaned1[11]), int(cleaned1[12]), int(cleaned1[13]), date_time))
                            db.commit()
                        except sqlite3.IntegrityError:
                            print sqlite3.Error
                        try:
                            date_time = str(datetime.datetime.now())
                            db.execute('''INSERT INTO NCAAstats(game_id, team, fgma, tpma, ftma, oreb, dreb, reb, ast, stl, blk, turnovers, pf, pts, timestamp ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''', (halftime_ids[i], team2, cleaned2[2], cleaned2[3], cleaned2[4], int(cleaned2[5]), int(cleaned2[6]), int(cleaned2[7]), int(cleaned2[8]), int(cleaned2[9]), int(cleaned2[10]), int(cleaned2[11]), int(cleaned2[12]), int(cleaned2[13]), date_time))
                            db.commit()
                        except sqlite3.IntegrityError:
                            print sqlite3.Error            
                    except sqlite3.IntegrityError:
                        print 'Record Already Exists'    
                except:
                    print 'No Boxscore Available'     
index()
db.close()


