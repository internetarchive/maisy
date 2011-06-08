import sys
import io
import datetime
import gp2ia

def blog ( str ):
    global logfile
    print str
    logfile.write( "%s\n" %  str.encode( 'utf-8' ))

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
        blog ( 'Starting batch for %s at %s' % ( postlist, lt ) )
        for aline in targfile:
            anID = aline.strip()
            istr =  str(itemcount).zfill(4)
            blog ('[%s] Posting %s...' % (istr, anID) )
            postargs = args[:]
            postargs.insert(0, anID)
            res = gp2ia.main(postargs)
            itemcount = itemcount + 1
            if res == 0:
                goodcount = goodcount + 1
                blog( '\t(0) Success!')
            elif res == 1:
                retcode = 1
                blog( '\t(1) Failed: post error(s)' )
            elif res == 2:
                retcode = 1
                blog( '\t(2) Failed: missing data' )
            else:
                retcode = 1
                blog( '\t(?) Unknown exit status ( %s ) % res' )
        lt = datetime.datetime.now()
        blog ( 'Posted %s of %s items successfully' % ( goodcount, itemcount) )
        blog ( 'Finished batch for %s at %s' % ( postlist, lt ) )

    targfile.close()
    return retcode

if __name__ == "__main__":
    sys.exit(main())