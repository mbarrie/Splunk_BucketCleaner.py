# Splunk_BucketCleaner.py

Steps:
1. Edit a file (the example uses a file called servers.list) with names/ip's of all the indexers you are intersted in tracking
2. run the script from some node like this:
        python ./findBuckets.py --basedir=/opt/splunk/var/lib/splunk --indexes=_internal,main --serverlist=servers.list --reverse

the --indexes flag is used to specify which indexes to use.  
the --restrictdirs flag is used to restrict the search to just the supplied db directories
the --reverse flag baically dictates if the script shows unique buckets (say for a backup script) or extra buckets (to safely delete from frozen)
