import MySQLdb
import csv
from users import Users


if __name__ == '__main__':

    conn = MySQLdb.connect(host='127.0.0.1',user='root',passwd='891028',db='github')
    cur = conn.cursor()
    with open("/Users/dreamteam/Documents/study/sonar/script/exp_repos.csv") as seed:
        reader = csv.reader(seed)
        next(reader,None)
        for line in reader:
            mUsers = Users(conn,cur,int(line[0]),line[1],line[2],"2016-03-01 00:00:00")
            mUsers.getLongAndShortUsers()
    cur.close()
    conn.close()