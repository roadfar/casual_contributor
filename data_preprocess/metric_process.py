import csv
import MySQLdb
import urllib2
import json


def deleteNULLRecords(path):
    writer=csv.writer(file('/Users/dreamteam/Documents/study/sonar/results/oss developer evaluation/all_cmnt_processed.csv','wb'))
    writer.writerow(['id','login','user_id','type','name','company','blog','location','email','hireable','bio','created_at','git_name','git_email','repo_id','CCGN','CMT','IRPT','GBG','GQI','CMNT','AAT','long_term','blocker','critical',	'major','minor','info','issue_tags','PCMNT','ICMNT','CCMNT','AGBG','AGQI'])
    with open(path) as seed:
        reader = csv.reader(seed)
        next(reader,None)
        for line in reader:
            CCGN = line[15]
            if CCGN != None and CCGN != '0':
                agbg = float(line[18])/float(line[15])
                agqi = float(line[19])/float(line[15])
                line.append(agbg)
                line.append(agqi)
                writer.writerow(line)

def getUserIssuesByTag(path):

    def update_info(value,repo_id,user_id):
        cur.execute("update users set info = '%s' where repo_id = '%d' and user_id = '%d';" % (value,repo_id,user_id))
        conn.commit()
    def update_minor(value,repo_id,user_id):
        cur.execute("update users set minor = '%s' where repo_id = '%d' and user_id = '%d';" % (value,repo_id,user_id))
        conn.commit()
    def update_major(value,repo_id,user_id):
        cur.execute("update users set major = '%s' where repo_id = '%d' and user_id = '%d';" % (value,repo_id,user_id))
        conn.commit()
    def update_critical(value,repo_id,user_id):
        cur.execute("update users set critical = '%s' where repo_id = '%d' and user_id = '%d';" % (value,repo_id,user_id))
        conn.commit()
    def update_blocker(value,repo_id,user_id):
        cur.execute("update users set blocker = '%s' where repo_id = '%d' and user_id = '%d';" % (value,repo_id,user_id))
        conn.commit()

    with open(path) as seed:
        reader = csv.reader(seed)
        next(reader,None)
        for line in reader:
            user_id = int(line[2])
            repo_id = int(line[14])
            git_email = str(line[13])
            print "updating "+git_email
            #get severity
            sonar_cur.execute("select severity, count(*) from issues where repo_id = '%d' and author_login = '%s' GROUP BY severity;" % (repo_id, git_email))
            results = sonar_cur.fetchall()
            for result in results:
                if result[0] == 'INFO':
                    update_info(result[1],repo_id,user_id)
                elif result[0] == 'MINOR':
                    update_minor(result[1],repo_id,user_id)
                elif result[0] == 'MAJOR':
                    update_major(result[1],repo_id,user_id)
                elif result[0] == 'CRITICAL':
                    update_critical(result[1],repo_id,user_id)
                elif result[0] == 'BLOCKER':
                    update_blocker(result[1],repo_id,user_id)

def getCountByTags(long_term):
    writer=csv.writer(file('/Users/dreamteam/Documents/study/sonar/results/oss developer evaluation/new/'+str(long_term)+'_tags_new.csv','wb'))
    sonar_cur.execute("select sonar.issues.tags, count(*) as num from sonar.issues, github.users WHERE github.users.long_term = '%d' and (github.users.repo_id = 2862290 or github.users.repo_id = 7508411 or github.users.repo_id = 22790488 or github.users.repo_id = 892275 or github.users.repo_id = 4839957 or github.users.repo_id = 10057936 or github.users.repo_id = 5152285 or github.users.repo_id = 3544424 or github.users.repo_id = 596892 or github.users.repo_id = 1362490 or github.users.repo_id = 1039520 or github.users.repo_id = 529502 or github.users.repo_id = 26516210 or github.users.repo_id = 7116973 or github.users.repo_id = 688157 or github.users.repo_id = 17341429 or github.users.repo_id = 2519670 or github.users.repo_id = 15495333 or github.users.repo_id = 3457006 or github.users.repo_id = 7190749 or github.users.repo_id = 5269397) and sonar.issues.tags is not NULL and sonar.issues.author_login = github.users.git_email group by sonar.issues.tags;" % long_term)
    results = sonar_cur.fetchall()
    tags = []
    for result in results:
        if ',' in result[0]:
            array = result[0].split(',')
            for item in array:
                if item not in tags:
                    tags.append(item)
        elif result[0] not in tags:
            tags.append(result[0])
    for tag in tags:
        total = 0
        for result in results:
            if tag in result[0]:
                total = total + int(result[1])
        row = (tag,str(total))
        writer.writerow(row)

def getCountByRules(long_term):
    writer=csv.writer(file('/Users/dreamteam/Documents/study/sonar/results/oss developer evaluation/new/'+str(long_term)+'_rules_new.csv','wb'))
    sonar_cur.execute("select sonar.issues.rule_id, count(*) as num from sonar.issues, github.users WHERE github.users.long_term = '%d' and sonar.issues.author_login = github.users.git_email group by sonar.issues.rule_id;" % long_term)
    results = sonar_cur.fetchall()
    tags = []
    for result in results:
        if ',' in result[0]:
            array = result[0].split(',')
            for item in array:
                if item not in tags:
                    tags.append(item)
        elif result[0] not in tags:
            tags.append(result[0])
    for tag in tags:
        total = 0
        for result in results:
            if tag in result[0]:
                total = total + int(result[1])
        row = (tag,str(total))
        writer.writerow(row)

def deleteSZZ(conn,cur,path):
    with open(path) as seed:
        reader = csv.reader(seed)
        next(reader,None)
        for line in reader:
            repo_id = line[1]
            sha = line[3]
            cur.execute("update commits set issue_id = NULL where sha = '%s' and repo_id = '%s';" % (str(sha), str(repo_id)))
            conn.commit()

def updateLongTerm(conn,cur,path):
    with open(path) as seed:
        reader = csv.reader(seed)
        next(reader,None)
        for line in reader:
            print "updating++++"+line[0]
            repo_id = line[0]
            count = int(line[1])*0.2
            cur.execute("update users set long_term = 1 where repo_id = '%s' and CMT > '%d';" % (str(repo_id), count))
            conn.commit()
            cur.execute("update users set long_term = 0 where repo_id = '%s' and CMT < '%d';" % (str(repo_id), count))
            conn.commit()

#get the number of all commits of a developer
def getAllCommitNum(login,page_num,deadline,commit_num,repo_i,commit_i,sha):
    print "***************** getting commits of " + login + "; repo_page_num: " + str(page_num)
    try:
        for number in range(page_num,100000):
            repo_url = "https://api.github.com/users/%s/repos?per_page=100&page=%d&access_token=%s" % (login,number,sha)
            request_content = urllib2.Request(repo_url)
            repo_result = urllib2.urlopen(request_content).read()
            if repo_result != "[]":
                repos = json.loads(repo_result)
                for i in range(repo_i,len(repos)):
                    print "**********the "+str(i)+"th repo"
                    full_name = str(repos[i]['full_name'])
                    for cmt_page_num in range(commit_i,100000):
                        print "the " + str(cmt_page_num) + "th commit: " + str(commit_num)
                        commit_url = "https://api.github.com/repos/%s/commits?until=%s&author=%s&per_page=100&page=%d&access_token=%s" % (full_name,deadline,login,cmt_page_num,sha)
                        re_content = urllib2.Request(commit_url)
                        cmt_result = urllib2.urlopen(re_content).read()
                        if cmt_result != "[]":
                            commits = json.loads(cmt_result)
                            commit_num = commit_num + len(commits)
                            if len(commits) < 100:
                                break
                        else:
                            break
                if len(repos) < 100:
                    break
            else:
                break
        cur.execute("update users set commits = '%d' where login = '%s';" % (commit_num,login))
        conn.commit()
    except MySQLdb.Error, e:
        print "Mysql Error!", e;

def getRepos(login,sha):
    print "***************** getting the repos of " + login
    all_repos = []
    for number in range(1,100000):
        repo_url = "https://api.github.com/users/%s/repos?per_page=100&page=%d&access_token=%s" % (login,number,sha)
        request_content = urllib2.Request(repo_url)
        repo_result = urllib2.urlopen(request_content).read()
        if repo_result != "[]":
            repos = json.loads(repo_result)
            for repo in repos:
                full_name = str(repo['full_name'])
                all_repos.append(full_name)
            if len(repos) < 100:
                break
        else:
            break

    for repo in all_repos:
        row = (repo,)
        writer.writerow(row)

if __name__ == "__main__":
    csvfile = file('/Users/dreamteam/Documents/study/sonar/results/oss developer evaluation/new/all_repos.csv','wb')
    writer = csv.writer(csvfile)
    conn = MySQLdb.connect(host='127.0.0.1',user='root',passwd='891028',db='github')
    cur = conn.cursor()
    sonar_conn = MySQLdb.connect(host='127.0.0.1',user='root',passwd='891028',db='sonar')
    sonar_cur = sonar_conn.cursor()
    # deleteNULLRecords("/Users/dreamteam/Documents/study/sonar/results/oss developer evaluation/all.csv")
    # updateLongTerm(conn,cur,"/Users/dreamteam/Documents/study/sonar/results/oss developer evaluation/commit_num.csv")
    # deleteSZZ(conn,cur,"/Users/dreamteam/Documents/study/sonar/script/read_to_delete_commits.csv")
    # deleteNULLRecords("/Users/dreamteam/Documents/study/sonar/results/oss developer evaluation/all_cmnt.csv")
    # getUserIssuesByTag('/Users/dreamteam/Documents/study/sonar/results/oss developer evaluation/all_processed.csv')
    # getCountByTags(0)
    # getCountByTags(0)

# 84bcaa2aba96644d2604f86da91baa0105c15cfa
# adaccd3708619221656a9e13fc77bd8e5270c70a
# c9b850151b3aeb6d750619653bdd25ad27e79e76
# d1e7a9d2eb0f54ec0e96ca06a64d68897d122513
# cd1c3cda5c269c30ca3527968ea1a4e41038d8f1
# 25826285cf58b8bea2712298f2bc1dc598c8200b
# 277152ee058361c684ee253b881141bba722dfc5
# 7933edb46487203f2752a93c702fd630cd552bae
# 9377749a42d8492428c52329624a8388b620e926
# d510a45755084273b25a12703eb5c39245cf4db3
# c023106576987273c64d45c8252cbc1785460917
# 1473e930e6a7674673b0e864711d12b2c38db5fd
# 2fcd8e5c4fcf05eea2fecd5bd617d4910c37881f
# 0e862a968c0f7ba84acc9b909c3fccdf3e9cd15a
# 877738b0ede13b627605e301dd4f00725697ca0d
# 8368357f10e6318309b7e278b900e375f73421bd
# 6ac08b18aa04a36b602957be808a813900487173
# e94b02919d147be8df7dfc5ede4de92c3a75b1b4

    with open("/Users/dreamteam/Documents/study/sonar/results/oss developer evaluation/new/logins.csv") as seed:
        reader = csv.reader(seed)
        next(reader,None)
        for line in reader:
            login = line[0]
            # getAllCommitNum(login,1,"2016-03-01T00:00:00Z",0,1,1,"84bcaa2aba96644d2604f86da91baa0105c15cfa")
            getRepos(login,"84bcaa2aba96644d2604f86da91baa0105c15cfa")