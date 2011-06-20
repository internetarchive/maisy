import sys
import io
import time
import datetime
import gp2ia

def blog ( str ):
    global logfile
    print str
    logfile.write( "%s\n" %  str.encode( 'utf-8' ))
    logfile.flush()

global logfile

def main():

    global logfile
    
    postlog = 'post_batch.log'
    
    args = sys.argv
    
    if len(args) < 2:
        print 'Usage: post_batch <filename>'
        return 2
    
    args.pop(0)  # this file name
    postlist = args.pop(0)
    logfn = '--log=%s.log' % postlist 
    args.insert(0, logfn)
    
    targfile = open (postlist, "r")
    
    retcode = 0
    itemcount = 0
    goodcount = 0
    
    with open( postlog, "a" ) as logfile:
        lt = datetime.datetime.now()
        blog ( '%s Starting batch for %s at %s' % ( lt, postlist, lt ) )
        for aline in targfile:
            anID = aline.strip()
            istr =  str(itemcount).zfill(4)
            lt = datetime.datetime.now()
            blog ('%s [%s] Posting %s...' % (lt, istr, anID) )
            postargs = args[:]
            postargs.insert(0, anID)
            res = gp2ia.main(postargs)
            itemcount = itemcount + 1
            lt = datetime.datetime.now()
            if res == 0:
                goodcount = goodcount + 1
                blog( '%s \t(0) Success!' % lt)
            elif res == 1:
                retcode = 1
                blog( '%s \t(1) Failed: post error(s)' % lt)
            elif res == 2:
                retcode = 1
                blog( '%s \t(2) Failed: missing data' % lt)
            else:
                retcode = 1
                blog( '%s \t(%s) Unknown exit status' % (lt, res ) )
            # time.sleep(5)
        lt = datetime.datetime.now()
        blog ( '%s Posted %s of %s items successfully' % ( lt, goodcount, itemcount) )
        blog ( '%s Finished batch for %s at %s' % ( lt, postlist, lt ) )

    targfile.close()
    return retcode

if __name__ == "__main__":
    sys.exit(main())