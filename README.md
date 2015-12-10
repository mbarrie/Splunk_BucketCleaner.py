# Splunk_BucketCleaner.py

Steps:
1. Edit a file (the example uses a file called servers.list) with names/ip's of all the indexers you are interested in tracking
2. run the script from some node like this:
        python ./findBuckets.py --basedir=/opt/splunk/var/lib/splunk --indexes=_internal,main --serverlist=servers.list --reverse

the --indexes flag is used to specify which indexes to use.  
the --restrictdirs flag is used to restrict the search to just the supplied db directories. You can give it a comma separated list of valid db directories (cold,colddb,thaweddb)
the --reverse flag baically dictates if the script shows unique buckets (say for a backup script) or extra buckets (to safely delete from frozen)
the --verbose flag makes things verbose.

the --searchFrozen flag says that instead of live buckets, you want to run it against some forzen bucket repository (specified by --basedir) on each node. It requires an extra flag --sshBlurb that is basically a hack to prepend something to each ssh line (to account for different ssh methods).  For example if you would normally ssh by running "ssh -i ~/mk_key.pem ec2-user@somehostname" then the sshBllurb would be"ssh -i ~/mk_key.pem ec2-user". Another instance might have shared keys with an sshblurb of just "ssh splunkuser".