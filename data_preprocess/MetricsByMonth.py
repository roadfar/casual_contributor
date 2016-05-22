# coding: UTF-8
import git
import MySQLdb
import os
import re
import json
import datetime
import csv


class MetricsByMonth:
    def __init__(self, conn, cur, repo_id, repo_name, deadline):
        self.conn = conn
        self.cur = cur
        self.repo_id = repo_id
        self.repo_name = repo_name
        self.deadline = deadline

    def getAllGithubUserIDs(self):
        print "getting all users of "+ self.repo_name
        self.cur.execute("select user_id from users where repo_id = '%d';" % self.repo_id)
        commit_comment_authors = self.cur.fetchall()
        return commit_comment_authors

    def getAllGithubUsers(self):
        print "getting all users of "+ self.repo_name
        self.cur.execute("select git_email,user_id from users where repo_id = '%d' and git_email is not null;" % self.repo_id)
        commit_comment_authors = self.cur.fetchall()
        return commit_comment_authors


    def exportGQI(self):
        array = ['user_login','2012-01','2012-02','2012-03','2012-04','2012-05','2012-06','2012-07','2012-08','2012-09','2012-10','2012-11','2012-12','2013-01','2013-02','2013-03','2013-04','2013-05','2013-06','2013-07','2013-08','2013-09','2013-10','2013-11','2013-12','2014-01','2014-02','2014-03','2014-04','2014-05','2014-06','2014-07','2014-08','2014-09','2014-10','2014-11','2014-12','2015-01','2015-02','2015-03','2015-04','2015-05','2015-06','2015-07','2015-08','2015-09','2015-10','2015-11','2015-12']
        git_month = ['2012-1-1','2012-2-1','2012-3-1','2012-4-1','2012-5-1','2012-6-1','2012-7-1','2012-8-1','2012-9-1','2012-10-1','2012-11-1','2012-12-1','2013-1-1','2013-2-1','2013-3-1','2013-4-1','2013-5-1','2013-6-1','2013-7-1','2013-8-1','2013-9-1','2013-10-1','2013-11-1','2013-12-1','2014-1-1','2014-2-1','2014-3-1','2014-4-1','2014-5-1','2014-6-1','2014-7-1','2014-8-1','2014-9-1','2014-10-1','2014-11-1','2014-12-1','2015-1-1','2015-2-1','2015-3-1','2015-4-1','2015-5-1','2015-6-1','2015-7-1','2015-8-1','2015-9-1','2015-10-1','2015-11-1','2015-12-1','2016-1-1']
        writer=csv.writer(file('/Users/dreamteam/Documents/study/sonar/results/oss developer evaluation/metricsbymonth/'+self.repo_name+'_gqi.csv','wb'))
        writer.writerow(array)
        users = self.getAllGithubUsers()
        path = "/Users/dreamteam/Documents/git_repos/Python/"+self.repo_name
        if os.path.isdir(path):
            os.chdir(path)
        else:
            print path+" is wrong !please check it again !"
        for user in users:
            if not user is None:
                print "exporting " + str(user)
                sonarConn = MySQLdb.connect(host='127.0.0.1',user='root',passwd='891028',db='sonar')
                self.cur.execute("select LEFT(github.commits.created_at, 7) as month, count(*) from sonar.issues, github.commits where sonar.issues.repo_id = '%d' and sonar.issues.author_login = '%s' and sonar.issues.commit_sha = github.commits.sha and github.commits.created_at BETWEEN '2012-01-01' and '2016-01-01' group by LEFT(github.commits.created_at, 7);" % (self.repo_id, str(user[0])))
                months = self.cur.fetchall()
                a = [0]*49
                a[0] = user[0]
                for i in range(0,len(array)-1):
                    code_efforts = os.popen("git log --author="+user[0]+" --all --since="+git_month[i]+" --before="+git_month[i+1]+" --pretty=tformat: --numstat | awk '{ add += $1 ; subs += $2 ; loc += $1 - $2 } END { printf \"added lines: \%s removed lines : \%s total lines: \%s\\n\",add,subs,loc }' - ").read()
                    add_remove_total = re.findall(r"\d+\.?\d*",code_efforts)
                    for month in months:
                    # write to csv
                        if array[i+1] == month[0] and len(add_remove_total)>0 and int(add_remove_total[0]) > 0:
                            a[i+1] = float(month[1])/float(add_remove_total[0])
                            break
                row = (a[0],a[1],a[2],a[3],a[4],a[5],a[6],a[7],a[8],a[9],a[10],a[11],a[12],a[13],a[14],a[15],a[16],a[17],a[18],a[19],a[20],a[21],a[22],a[23],a[24],a[25],a[26],a[27],a[28],a[29],a[30],a[31],a[32],a[33],a[34],a[35],a[36],a[37],a[38],a[39],a[40],a[41],a[42],a[43],a[44],a[45],a[46],a[47],a[48])
                # print row
                writer.writerow(row)
            else:
                print "find a none user!"

    def exportIRPT(self):
        array = ['2013-01','2013-02','2013-03','2013-04','2013-05','2013-06','2013-07','2013-08','2013-09','2013-10','2013-11','2013-12','2014-01','2014-02','2014-03','2014-04','2014-05','2014-06','2014-07','2014-08','2014-09','2014-10','2014-11','2014-12','2015-01','2015-02','2015-03','2015-04','2015-05','2015-06','2015-07','2015-08','2015-09','2015-10','2015-11','2015-12']
        writer=csv.writer(file('/Users/dreamteam/Documents/study/sonar/results/oss developer evaluation/metricsbymonth/'+self.repo_name+'_irpt.csv','wb'))
        writer.writerow(array)
        users = self.getAllGithubUserIDs()
        for user in users:
            if not user is None:
                print "exporting " + str(user)
                self.cur.execute("select LEFT(created_at, 7) as month, count(*) from issues where repo_id = '%d' and author_id = '%d' and created_at BETWEEN '2012-01-01' and '2016-03-01' group by LEFT(created_at, 7);" % (self.repo_id, user))
                months = self.cur.fetchall()
                row = ""
                for a in array:
                    tag = 0
                    for month in months:
                    # write to csv
                        if a == month[0]:
                            row = row + str(month[1])
                            tag = 1
                            break
                    if tag == 0:
                        row = row + "0"
                # print row
                writer.writerow(row)
            else:
                print "find a none user!"

    def exportGBG(self):
        array = ['author_id','2012-01','2012-02','2012-03','2012-04','2012-05','2012-06','2012-07','2012-08','2012-09','2012-10','2012-11','2012-12','2013-01','2013-02','2013-03','2013-04','2013-05','2013-06','2013-07','2013-08','2013-09','2013-10','2013-11','2013-12','2014-01','2014-02','2014-03','2014-04','2014-05','2014-06','2014-07','2014-08','2014-09','2014-10','2014-11','2014-12','2015-01','2015-02','2015-03','2015-04','2015-05','2015-06','2015-07','2015-08','2015-09','2015-10','2015-11','2015-12']
        git_month = ['2012-1-1','2012-2-1','2012-3-1','2012-4-1','2012-5-1','2012-6-1','2012-7-1','2012-8-1','2012-9-1','2012-10-1','2012-11-1','2012-12-1','2013-1-1','2013-2-1','2013-3-1','2013-4-1','2013-5-1','2013-6-1','2013-7-1','2013-8-1','2013-9-1','2013-10-1','2013-11-1','2013-12-1','2014-1-1','2014-2-1','2014-3-1','2014-4-1','2014-5-1','2014-6-1','2014-7-1','2014-8-1','2014-9-1','2014-10-1','2014-11-1','2014-12-1','2015-1-1','2015-2-1','2015-3-1','2015-4-1','2015-5-1','2015-6-1','2015-7-1','2015-8-1','2015-9-1','2015-10-1','2015-11-1','2015-12-1','2016-1-1']
        writer=csv.writer(file('/Users/dreamteam/Documents/study/sonar/results/oss developer evaluation/metricsbymonth/'+self.repo_name+'_gbg.csv','wb'))
        writer.writerow(array)
        self.cur.execute("select git_email,user_id from users where repo_id = '%d' and git_email is not null;" % self.repo_id)
        users = self.cur.fetchall()
        path = "/Users/dreamteam/Documents/git_repos/Python/"+self.repo_name
        if os.path.isdir(path):
            os.chdir(path)
        else:
            print path+" is wrong !please check it again !"
        for user in users:
            if not user is None:
                print "exporting " + str(user)
                self.cur.execute("select LEFT(commits.created_at, 7) as month, count(distinct szz_raw.issue_id) from commits,szz_raw where szz_raw.proj_id = '%d' and szz_raw.innocent = 0 and szz_raw.buggy_sha = commits.sha and commits.author_id = '%d' and commits.created_at BETWEEN '2012-01-01' and '2016-03-01' group by LEFT(commits.created_at, 7);" % (self.repo_id, user[1]))
                months = self.cur.fetchall()
                a = [0]*49
                a[0] = user[0]
                for i in range(0,len(array)-1):
                    # if os.path.isdir(path):
                    #     os.chdir(path)
                    # else:
                    #     print path+" is wrong !please check it again !"
                    code_efforts = os.popen("git log --author="+user[0]+" --all --since="+git_month[i]+" --before="+git_month[i+1]+" --pretty=tformat: --numstat | awk '{ add += $1 ; subs += $2 ; loc += $1 - $2 } END { printf \"added lines: \%s removed lines : \%s total lines: \%s\\n\",add,subs,loc }' - ").read()
                    add_remove_total = re.findall(r"\d+\.?\d*",code_efforts)
                    for month in months:
                    # write to csv
                        if array[i+1] == month[0] and len(add_remove_total)>0:
                            a[i+1] = float(month[1])/float(add_remove_total[0])
                            break
                row = (a[0],a[1],a[2],a[3],a[4],a[5],a[6],a[7],a[8],a[9],a[10],a[11],a[12],a[13],a[14],a[15],a[16],a[17],a[18],a[19],a[20],a[21],a[22],a[23],a[24],a[25],a[26],a[27],a[28],a[29],a[30],a[31],a[32],a[33],a[34],a[35],a[36],a[37],a[38],a[39],a[40],a[41],a[42],a[43],a[44],a[45],a[46],a[47],a[48])
                print row
                writer.writerow(row)
            else:
                print "find a none user!"

    def exportCCGN(self):
        array = ['author_id','2012-01','2012-02','2012-03','2012-04','2012-05','2012-06','2012-07','2012-08','2012-09','2012-10','2012-11','2012-12','2013-01','2013-02','2013-03','2013-04','2013-05','2013-06','2013-07','2013-08','2013-09','2013-10','2013-11','2013-12','2014-01','2014-02','2014-03','2014-04','2014-05','2014-06','2014-07','2014-08','2014-09','2014-10','2014-11','2014-12','2015-01','2015-02','2015-03','2015-04','2015-05','2015-06','2015-07','2015-08','2015-09','2015-10','2015-11','2015-12']
        git_month = ['2012-1-1','2012-2-1','2012-3-1','2012-4-1','2012-5-1','2012-6-1','2012-7-1','2012-8-1','2012-9-1','2012-10-1','2012-11-1','2012-12-1','2013-1-1','2013-2-1','2013-3-1','2013-4-1','2013-5-1','2013-6-1','2013-7-1','2013-8-1','2013-9-1','2013-10-1','2013-11-1','2013-12-1','2014-1-1','2014-2-1','2014-3-1','2014-4-1','2014-5-1','2014-6-1','2014-7-1','2014-8-1','2014-9-1','2014-10-1','2014-11-1','2014-12-1','2015-1-1','2015-2-1','2015-3-1','2015-4-1','2015-5-1','2015-6-1','2015-7-1','2015-8-1','2015-9-1','2015-10-1','2015-11-1','2015-12-1','2016-1-1']
        writer=csv.writer(file('/Users/dreamteam/Documents/study/sonar/results/oss developer evaluation/metricsbymonth/'+self.repo_name+'_gbg.csv','wb'))
        writer.writerow(array)
        path = "/Users/dreamteam/Documents/git_repos/Python/"+self.repo_name
        if os.path.isdir(path):
            os.chdir(path)
        else:
            print path+" is wrong !please check it again !"
        users = self.getAllGithubUsers()
        for user in users:
            if not user is None:
                print "exporting " + str(user)
                a = [0]*49
                a[0] = user[0]
                for i in range(0,len(array)-1):
                    # if os.path.isdir(path):
                    #     os.chdir(path)
                    # else:
                    #     print path+" is wrong !please check it again !"
                    code_efforts = os.popen("git log --author="+user[0]+" --all --since="+git_month[i]+" --before="+git_month[i+1]+" --pretty=tformat: --numstat | awk '{ add += $1 ; subs += $2 ; loc += $1 - $2 } END { printf \"added lines: \%s removed lines : \%s total lines: \%s\\n\",add,subs,loc }' - ").read()
                    add_remove_total = re.findall(r"\d+\.?\d*",code_efforts)
                    for month in months:
                    # write to csv
                        if array[i+1] == month[0] and len(add_remove_total)>0:
                            a[i+1] = float(month[1])/float(add_remove_total[0])
                            break
                row = (a[0],a[1],a[2],a[3],a[4],a[5],a[6],a[7],a[8],a[9],a[10],a[11],a[12],a[13],a[14],a[15],a[16],a[17],a[18],a[19],a[20],a[21],a[22],a[23],a[24],a[25],a[26],a[27],a[28],a[29],a[30],a[31],a[32],a[33],a[34],a[35],a[36],a[37],a[38],a[39],a[40],a[41],a[42],a[43],a[44],a[45],a[46],a[47],a[48])
                print row
                writer.writerow(row)
            else:
                print "find a none user!"

    def exportAAT(self):
        array = ['author_id','2012-01','2012-02','2012-03','2012-04','2012-05','2012-06','2012-07','2012-08','2012-09','2012-10','2012-11','2012-12','2013-01','2013-02','2013-03','2013-04','2013-05','2013-06','2013-07','2013-08','2013-09','2013-10','2013-11','2013-12','2014-01','2014-02','2014-03','2014-04','2014-05','2014-06','2014-07','2014-08','2014-09','2014-10','2014-11','2014-12','2015-01','2015-02','2015-03','2015-04','2015-05','2015-06','2015-07','2015-08','2015-09','2015-10','2015-11','2015-12']
        writer=csv.writer(file('/Users/dreamteam/Documents/study/sonar/results/oss developer evaluation/metricsbymonth/'+self.repo_name+'_aat.csv','wb'))
        writer.writerow(array)
        self.cur.execute("select user_id from users where repo_id = '%d' and user_id is not null;" % self.repo_id)
        users = self.cur.fetchall()
        for user in users:
            print "exporting " + str(user[0])
            self.cur.execute("select LEFT(commits.created_at, 7) as month, count(distinct szz_raw.issue_id) from commits,szz_raw where szz_raw.proj_id = '%d' and szz_raw.innocent = 0 and szz_raw.buggy_sha = commits.sha and commits.author_id = '%d' and commits.created_at BETWEEN '2012-01-01' and '2016-01-01' group by LEFT(commits.created_at, 7);" % (self.repo_id, user[1]))
            months = self.cur.fetchall()
            a = [0]*49
            a[0] = user[0]
            for i in range(0,len(array)-1):
                # if os.path.isdir(path):
                #     os.chdir(path)
                # else:
                #     print path+" is wrong !please check it again !"
                code_efforts = os.popen("git log --author="+user[0]+" --all --since="+git_month[i]+" --before="+git_month[i+1]+" --pretty=tformat: --numstat | awk '{ add += $1 ; subs += $2 ; loc += $1 - $2 } END { printf \"added lines: \%s removed lines : \%s total lines: \%s\\n\",add,subs,loc }' - ").read()
                add_remove_total = re.findall(r"\d+\.?\d*",code_efforts)
                for month in months:
                # write to csv
                    if array[i+1] == month[0] and len(add_remove_total)>0:
                        a[i+1] = float(month[1])/float(add_remove_total[0])
                        break
            row = (a[0],a[1],a[2],a[3],a[4],a[5],a[6],a[7],a[8],a[9],a[10],a[11],a[12],a[13],a[14],a[15],a[16],a[17],a[18],a[19],a[20],a[21],a[22],a[23],a[24],a[25],a[26],a[27],a[28],a[29],a[30],a[31],a[32],a[33],a[34],a[35],a[36],a[37],a[38],a[39],a[40],a[41],a[42],a[43],a[44],a[45],a[46],a[47],a[48])
            print row
            writer.writerow(row)

if __name__ == '__main__':
    conn = MySQLdb.connect(host='127.0.0.1',user='root',passwd='891028',db='github')
    cur = conn.cursor()
    mMetrics = MetricsByMonth(conn,cur,3544424,"httpie","2016-01-01 00:00:00")
    mMetrics.exportGQI()
    mMetrics.exportGBG()