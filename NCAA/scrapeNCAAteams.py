__author__ = 'tanyacashorali'

import jsonpickle
import random
import urllib2
import time
import re
import random
from datetime import date
import os
import sqlite3
import pandas as pd
from urlparse import urlparse
from bs4 import BeautifulSoup as bs

db = sqlite3.connect('/home/ec2-user/sports2015/NCAA/sports.db')


x=random.randint(1, 20)
time.sleep(x)


def f7(seq):
    seen = set()
    seen_add = seen.add
    return [ x for x in seq if not (x in seen or seen_add(x))]

def index():
    print "entered index"
    times = []
    halftime_ids = []
    today = date.today()
    today = today.strftime("%Y%m%d")
    #vals = [50,55,56,100]
    vals = [0,50,8,4,21]
    for vi in range(0,len(vals)):
        url = urllib2.urlopen('http://espn.go.com/mens-college-basketball/scoreboard/_/group/' + str(vals[vi]) + '/date/' + today)
        soup = bs(url.read(), ['fast', 'lxml'])
        data=re.search('window.espn.scoreboardData.*{(.*)};</script>', str(soup)).group(0)
        jsondata=re.search('({.*});window', data).group(1)
        j=jsonpickle.decode(jsondata) 
        print "entered index" + "conf=" + str(vals[vi])
        games = j['events']
        teams1 = [g['competitions'][0]['competitors'][0]['team']['abbreviation'] for g in games]
        teams2 = [g['competitions'][0]['competitors'][1]['team']['abbreviation'] for g in games]
        teams = teams1 + teams2
	ids1list = [re.findall('id\/(\d+)', str(g['competitions'][0]['competitors'][0]['team'])) for g in games]
        ids1 = [item for sublist in ids1list for item in sublist]
        ids1 = f7(ids1)
        ids2list = [re.findall('id\/(\d+)', str(g['competitions'][0]['competitors'][1]['team'])) for g in games]
        ids2 = [item for sublist in ids2list for item in sublist]
        ids2 = f7(ids2)
        ids =  ids1 + ids2
	for i in range(0, len(ids)):
            x=random.randint(5, 15)
            time.sleep(x)
            espn = 'http://espn.go.com/mens-college-basketball/team/stats/_/id/' + ids[i]
            print espn
            url1 = urllib2.urlopen(espn)
            soup1 = bs(url1.read(), ['fast', 'lxml'])
            team = teams[i]
            if team == 'WM':
                team = 'W&M'
            if team == 'TAM':
                team = 'TA&M'
            ## e.g. no season stats for http://espn.go.com/mens-college-basketball/team/stats/_/id/2395
            try:
                totals0 = soup1.find_all('tr', {'class':'total'})[0]
                data0 = totals0.findAll('td')
                values0 = [d.text for d in data0]
                cols0 = soup1.findAll('tr', {'class':'colhead'})[0]
                colnames0 = cols0.findAll('a')
                colnames0 = [c.text for c in colnames0]

                totals1 = soup1.find_all('tr', {'class':'total'})[1]
                data = totals1.findAll('td')
                values = [d.text for d in data]
                cols = soup1.findAll('tr', {'class':'colhead'})[1]
                colnames = cols.findAll('a')
                colnames = [c.text for c in colnames]
                try:
                    with db:
                        db.execute('''INSERT INTO NCAAseasonstats(team, the_date, min, fgm, fga, ftm, fta, tpm, tpa, pts, offr, defr, reb, ast, turnovers, stl, blk) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''', (team, time.strftime("%m/%d/%Y"),values[1],int(values[2]),int(values[3]),int(values[4]),int(values[5]),int(values[6]),int(values[7]),int(values[8]),int(values[9]),int(values[10]),int(values[11]),int(values[12]),int(values[13]),int(values[14]),int(values[15])))
                        db.commit()
                except sqlite3.IntegrityError:
                    print 'Team1 Season Stats Record Exists'
                try:
                    with db:
                        db.execute('''INSERT INTO NCAAseasontotals(team, the_date, gp, min, ppg, rpg, apg, spg, bpg, tpg, fgp, ftp, tpp) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)''', (team, time.strftime("%m/%d/%Y"), int(values0[1]), values0[2], int(values0[3]), int(values0[4]), int(values0[5]), int(values0[6]), int(values0[7]), int(values0[8]), float(values0[9]), float(values0[10]), float(values0[11])))
                        db.commit()
                except sqlite3.IntegrityError:
                    print 'Team1 Season Totals Record Exists'   
            except:
                print 'No stats for team'


index()
db.close()


