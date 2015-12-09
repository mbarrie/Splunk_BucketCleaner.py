import subprocess
import sys
import argparse
import os
import re

parser = argparse.ArgumentParser(description='Enumerate duplicate/redundant buckets')
parser.add_argument('--basedir', dest='basedir',nargs=1, required=True,
        help='the directory where indexes are stored on each host, defaults to /opt/splunk/var/lib/splunk')
parser.add_argument('--indexes', dest='indexlist',nargs=1, required=True,help='A comma separated list of indexes to enumerate')
parser.add_argument('--serverlist', dest='serverlist', nargs=1,required=True,help='A list of indexers to enumerate across')
parser.add_argument('--reverse', dest='reverse', action='store_true', default=False,
        help='Instead of listing unqiue buckets, list duplicates')
parser.add_argument('--verbose', dest='verbose', action='store_true',default=False,
        help='write debugging information to stdout')
parser.add_argument('--searchFrozen', dest='searchfrozen', action='store_true',default=False,
	help='ignore the actual dat store and look for forzen buckets in the basedir')
parser.add_argument('--sshBlurb', dest='sshblurb',nargs=1,required=False,help='the base string use to send ssh commands up to but not including host name, for example "ssh -i ~/.ssh/mykey ec2-user@"')
parser.add_argument('--restrictdirs',dest='ignoredirs',required=False, nargs=1, help='A comma separated list of dbNames to restict the search to [db|colddb|thaweddb]')
parser.add_argument('--test',dest='debugtest',nargs=1,required=False, help='For debugging: Use supplied directory as a top level local testing dir')
args = parser.parse_args()

basedir=args.basedir
reverse=args.reverse
verbose=args.verbose
searchFrozenOnly=args.searchfrozen
debugtest=args.debugtest

if (len(basedir) <= 0):
        basedir="/opt/splunk/var/lib/splunk"

indexList=args.indexlist[0].split(",")
serverFileName=args.serverlist
if (not None == args.ignoredirs):
    ignoreDirs=args.ignoredirs[0].split(",")
else:
    ignoreDirs=""
#print "indexFileName:" + indexFileName[0] + ", serverFileName:" + serverFileName[0]
print "Ignore Dirs:"
for ignore in ignoreDirs:
        print ignore

# Read in the server and index list 
with open(serverFileName[0]) as f:
        serverList = f.read().splitlines()

if len(serverList) == 0:
        print "The ServerList file was empty (or the file %s was incorrect), there must be at least one server in the list even if it is the local host" %(serverFileName)

        exit(1)
if len(indexList) == 0:
        print "The IndexList was empty, this won't produce any data" %(serverFileName)
        exit(1)


if (verbose):
        print "Enumerating the following indexes:"
        for p in indexList:
                print p
        print "Enumerating across the following servers:"
        for p in serverList:
                print p

def getBucketsFromServer(host,idxName, basedir,dbList,searchFrozenOnly):
        # | dbinspect index=_internal | fields  path | rex field=path ".+?\/(?<idxName>[^\/]+)\/(?<dbName>[^\/]+)\/(?<bucketName>\w{2}_\d+_.+)" | 
        # fields bucketName
        #splunk/bin/splunk search '| dbinspect index=_internal | fields  path | rex 
        # field=path ".+?\/(?<idxName>[^\/]+)\/(?<dbName>[^\/]+)\/(?<bucketName>\w{2}_\d+_.+)" | fields bucketName' -uri https://127.0.0.1:8089
        dbStr=""
        if (len(dbList) > 0):
            for db in dbList:
                dbStr = " OR dbName=" + db + dbStr
            dbStr=dbStr[3:]
	if(searchFrozenOnly):
		callString="%s@%s \"/usr/bin/find %s -name '*_*_*_*_*' -exec basename {} \; \"" %(args.sshblurb[0],host,basedir[0])
		print callString
	else:
        	callString="/opt/splunk/bin/splunk search '| dbinspect index=%s | fields  path | rex field=path \".+?\/(?<idxName>[^\/]+)\/(?<dbName>[^\/]+)\/(?<bucketName>\w{2}_\d+_\d+_\d+_.+)\" | search bucketName=* %s | fields bucketName' -uri https://%s:8089 -header F" %(idxName,dbStr,host)
        del_p = subprocess.Popen(callString,shell=True , stdout=subprocess.PIPE)
        out,err = del_p.communicate()
        retList = out.splitlines()
        print retList
        return retList

def getBucketsFromServer_test(host,idxName, basedir, debugpath):
        basedir = debugpath[0] + "/" + host +  basedir[0] + "/" + idxName
        find_string="/usr/bin/find %s -name  \"*_*_*\" -exec basename {} \;" %(basedir)

        del_p = subprocess.Popen(find_string,shell=True , stdout=subprocess.PIPE)
        out,err = del_p.communicate()
        retList = out.splitlines()
        outList = []
        for o in retList:
                outList.append(host + "|" + idxName + "|" + o)
        return outList

#This will provide comparision of justthe GUID portion of the bucket name.
def bucket_compare(x, y):
        if x[x.rfind("_")+1:] == y[y.rfind("_")+1:]:
                return local_id_compare(x,y)
        if x[x.rfind("_")+1:] > y[y.rfind("_")+1:]:
                return 1;
        else:
                return -1;

#Then compare the loccal_id
def local_id_compare(x, y):
        p = re.compile('([^_]+)_([^_]+)_([^_]+)_([^_]+)_([^_]+)', re.IGNORECASE)
        m = p.match(x)
        n = p.match(y)
        #print "%s %s" %(m.group(4),n.group(4))
        if m.group(4) == n.group(4):
                return prefix_compare(x,y)
        if m.group(4) > n.group(4):
                return 1;
        else:
                return -1;
#Finally compare the prefix (need to lop off the path first)
def prefix_compare(x, y):
        p = re.compile('([^_]+)_([^_]+)_([^_]+)_([^_]+)_([^_]+)', re.IGNORECASE)
        m = p.match(x)
        n = p.match(y)
        left = m.group(1)[-2:]
        right = n.group(1)[-2:]
        # print "%s %s" %(left,right)
        if (left == "db" and right == "db"):
                return 0;
        if (left =="rb" and  right == "db"):
                return 1;
        else:
                return -1;


def log_message(msg):
        logger_str="logger -s 'command:%s'" %(msg)
        logger_p = subprocess.Popen(logger_str,shell=True , stdout=subprocess.PIPE)
        out,err = logger_p.communicate()

def verbose_message(msg):
        if(verbose):
                print msg;
for idxName in indexList:
        servers = []
        for host in serverList:
                print "Examining index " + idxName + " on server:" + host
                if (debugtest == None):
                        servers = servers + getBucketsFromServer(host,idxName,basedir,ignoreDirs,searchFrozenOnly)
                else:
                        servers = servers + getBucketsFromServer_test(host,idxName,basedir, debugtest)
        servers.sort(cmp=bucket_compare)
        prevSubString = None
        delete_bucket_count = 0
        bucket_count = 0
        print "reverse:" + str(reverse)
        for p in servers:
                thisSubStr=p[p.find("_")+1:]
                if prevSubString != thisSubStr or prevSubString is None:
                        if reverse == False:
                                print "%s" % (p)
                                bucket_count += 1
                else:
                        delete_bucket_count += 1
                        bucket_count += 1
                        basefile = p
                        if reverse == True:
                                print "%s" % (p)

                prevSubString=p[p.find("_")+1:]

