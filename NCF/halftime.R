library(randomForest)
library(data.table)
library(RSQLite)
library(plyr)
library(dplyr)

drv <- dbDriver("SQLite")
con <- dbConnect(drv, "/home/ec2-user/sports2015/NCF/sports.db")

tables <- dbListTables(con)

lDataFrames <- vector("list", length=length(tables))

## create a data.frame for each table
for (i in seq(along=tables)) {
  if(tables[[i]] == 'NCFSBHalfLines' | tables[[i]] == 'NCFSBLines'){
  lDataFrames[[i]] <- dbGetQuery(conn=con, statement=paste("SELECT away_team, home_team, game_date, line, spread, max(game_time) as
game_time from ", tables[[i]], " group by away_team, home_team, game_date;"))

  } else {
    lDataFrames[[i]] <- dbGetQuery(conn=con, statement=paste("SELECT * FROM '", tables[[i]], "'", sep=""))
  }
  cat(tables[[i]], ":", i, "\n")
}

halflines <- lDataFrames[[which(tables == "NCFSBHalfLines")]]
lines <- lDataFrames[[which(tables == "NCFSBLines")]]
lookup <- lDataFrames[[which(tables == "NCFSBTeamLookup")]]
halfbox <- lDataFrames[[which(tables == "halfBoxScore")]]
finalbox <- lDataFrames[[which(tables == "finalBoxScore")]]
games <- lDataFrames[[which(tables == "games")]]

## Add ESPN abbreviations to line data
halflines$away_espn<-lookup[match(halflines$away_team, lookup$sb_team),]$espn_abbr
halflines$home_espn<-lookup[match(halflines$home_team, lookup$sb_team),]$espn_abbr
lines$away_espn<-lookup[match(lines$away_team, lookup$sb_team),]$espn_abbr
lines$home_espn<-lookup[match(lines$home_team, lookup$sb_team),]$espn_abbr
halflines <- halflines[c("away_espn", "home_espn", "line", "game_date")]
lines <- lines[c("away_espn", "home_espn", "line", "spread", "game_date")]

## Join games and half lines data
games$game_date<-substr(games$game_date,0,10)
games$key <- paste(games$team1, games$team2, games$game_date)
halflines$key <- paste(halflines$away_espn, halflines$home_espn, halflines$game_date)
games <- merge(halflines, games)

lines$key <- paste(lines$away_espn, lines$home_espn, lines$game_date)
games <- merge(lines, games, by="key")

halfbox <- merge(games, halfbox)[c("game_id", "game_date.x", "away_espn.x", "home_espn.x", "team", "line.x", "spread", "line.y", "first_downs",
            "third_downs", "fourth_downs", "total_yards", "passing", "comp_att", "yards_per_pass", "rushing", "rushing_attempts",
            "yards_per_rush","penalties","turnovers", "fumbles_lost","ints_thrown", "possession", "score")]
halfbox <- halfbox[order(halfbox$game_id),]
halfbox$tempteam <- ""
halfbox$tempteam[which(halfbox$team == halfbox$away_espn)] <- "team1"
halfbox$tempteam[which(halfbox$team != halfbox$away_espn)] <- "team2"

wide<-reshape(halfbox[,c(-3:-4)], direction = "wide", idvar="game_id", timevar="tempteam")
wide$first.half.points <- wide$score.team1 + wide$score.team2
colnames(wide)[6] <- "half.line"
wide$half.line <- as.numeric(wide$half.line)
wide$score.diff<-wide$score.team1 - wide$score.team2
wide$spread.team1 <- as.numeric(gsub("\\+", "", wide$spread.team1)) * -1
wide$abs.score.diff <- abs(wide$score.diff)
wide$abs.spread <- abs(wide$spread.team1)

load("rf.Rdat")

w <- wide
#w$second.half.score.team1 <- w$score.y.team1  -  w$score.team1
#w$second.half.score.team2 <- w$score.y.team2  -  w$score.team2

w$half.line<-as.numeric(as.character(w$line.y))
#w$second.half.points <- w$second.half.score.team1 + w$second.half.score.team2
w$first.half.points <- w$score.team1 + w$score.team2

#w$Over <- w$second.half.points > w$half.line
#w <- w[-which(is.na(w$Over)),]
#w$Over<-as.factor(w$Over)

#w$half.line <- as.numeric(as.character(w$line.y))
#w$line.x.team1<-as.numeric(w$line.x.team1)
#w$mwt <- w$score.team1 + w$score.team2 + (w$half.line - w$line.x.team1)
w$team1.favorite <- w$spread.team1 < 0
w$favorite.score <- 0
w$underdog.score <- 0

#z<-head(w[,c(1,70,96,35,128:130,13,48 )])
w$favorite.score[which(w$team1.favorite == TRUE)] <- w[which(w$team1.favorite == TRUE),]$score.team1
w$favorite.score[which(w$team1.favorite == FALSE)] <- w[which(w$team1.favorite == FALSE),]$score.team2

w$underdog.score[which(w$team1.favorite == FALSE)] <- w[which(w$team1.favorite == FALSE),]$score.team1
w$underdog.score[which(w$team1.favorite == TRUE)] <- w[which(w$team1.favorite == TRUE),]$score.team2

w$favorite.yards <- 0
w$underdog.yards <- 0
w$favorite.yards[which(w$team1.favorite == TRUE)] <- w[which(w$team1.favorite == TRUE),]$total_yards.team1
w$favorite.yards[which(w$team1.favorite == FALSE)] <- w[which(w$team1.favorite == FALSE),]$total_yards.team2

w$underdog.yards[which(w$team1.favorite == FALSE)] <- w[which(w$team1.favorite == FALSE),]$total_yards.team1
w$underdog.yards[which(w$team1.favorite == TRUE)] <- w[which(w$team1.favorite == TRUE),]$total_yards.team2

w$yards.diff <- w$favorite.yards - w$underdog.yards
w$score.diff <- w$favorite.score - w$underdog.score
w$favorite.trailing <- w$score.diff < 0


w$over.prediction <- predict(r, w)
x <- w[order(w$game_date.x.team2),c("game_id", "game_date.x.team2", "score.diff", "first.half.points", "half.line", "spread.team1", "over.prediction", "team.team1", "team.team2")]
x[which(x$game_date.x.team2 == "10/31/2015"),]
