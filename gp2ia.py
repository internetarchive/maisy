# TODO:
#  add tests for us being cut off by PG -- do NOT post HTML rate limiting message as content(!)
#  move retry logic to post_batch, retry lists should be handled there!
#  how best to handle corrupt RDF error -- should there be a forceDownload?
#  figure out better logic to queue derive on retry-- multiple files means multiple derives
# NOTES:
#  currently have curl retry set to 5 for posting through S3, can make things slow...
# CLEANUP:
#  symbolize retlog status codes instead of duping strings

import io
import os
import sys
import shutil
import string
import time
import httplib
from urlparse import urlparse 
import urllib2
import subprocess
import datetime
import codecs

# point to the actual functional version of Python in the petabox tree :P
sys.path.append('/petabox/sw/lib/lxml/lib/python2.5/site-packages')

from lxml import etree
from curses.ascii import isascii
from curses.ascii import isprint

def fetchGutenbergText ( etextID, forceUpload=False, dryrun=True ):
    """Fetch the Gutenberg.org resource with their etext ID into a directory we create with the same name.
    Returns atuple = ( etextID, archiveItemID, et, filesToPost, itemDir ) if ready to post, else None 
    """

    # NOTE: dryrun does not prevent http calls to GP or file retrieval and dir creation, only posting!

    global NSMap

    lt = datetime.datetime.now()
    dlog( 1, '---- SCRAPING eText %s at: %s' % (etextID, lt) )

    # Does this item already exist in the Archive? We should check for updates, not create...
    existsNow = archiveItem_exists( etextID )
    if not dryrun and existsNow is True:
        if forceUpload is False:
            dlog( 1, 'SKIPPING ITEM: call_number exists ( gutenberg etext# %s)' % etextID )
            return None

    proxyItemID = 'unknowntitle' + etextID + 'gut'  # for error cases

    # item does not seem to exist, create a local temp cache for all its files
    itemDir = tempDirForEtext( etextID )  

    # get RDF file containing metadata and available formats
    # filename following current PG convention, ideally we would get this from Location :P
    localFileName = 'pg%s.rdf' % etextID
    iurl = 'http://www.gutenberg.org/ebooks/%s.rdf' % etextID
    fullLocalFileName = itemDir + localFileName
    if fileExists( fullLocalFileName ) is False:  
        dlog( 2, 'Retrieving %s' % iurl )
        dlog( 2, 'Saving as %s' % localFileName )
        curllist = ['curl', '--header','User-Agent:ximm@archive.org (415) 637-5243 :)','--retry','5','-L','--verbose','-o', fullLocalFileName, iurl]
        p = subprocess.Popen( curllist, stderr = subprocess.STDOUT, stdout = subprocess.PIPE )
        (res, err) = p.communicate()
        exitcode = p.wait()
        if fileExists( fullLocalFileName ) is False:  
        #if exitcode != 0:
            dlog( 1, 'FAILED: Problem retrieving RDF file for %s (%s)'  % (etextID, fullLocalFileName) )
            dlog (1, 'Returned with exitcode %s and message: %s' % (exitcode, res))
            dlog (1, 'Error: %s' % err)
            retlog( '%s\t%s\t%s\t%s\t1' % ( 'missingRDF', proxyItemID, localFileName, iurl ) )      
            return None
    else:
        dlog( 1, '(Already have %s)' % localFileName )
    
    filesToPost = [ localFileName ]
   
    et = etree.parse ( fullLocalFileName ) 
        
    NSMap = et.getroot().nsmap
    
    if 'rdf' in NSMap is False:
        dlog( 2, 'FAILED. Retrieved RDF file for %s (%s) is corrupt?' % (etextID, fullLocalFileName) )
        retlog( '%s\t%s\t%s\t%s\t%s' % ( 'corruptRDF', proxyItemID, localFileName, iurl, '1' ) )      
        return None      
    
    t_resource = nsrdf ( 'resource')
    t_about = nsrdf ( 'about')
    t_value = nsrdf ( 'value')
    
    dcmi_ID = 'ebooks/' + etextID 

    # Sanity check: make sure this file is about the resource we think it is.
    ebookEntry = et.find( nspgterms ('ebook') )
    if ebookEntry.get( t_about ) != dcmi_ID:
        dlog( 2, 'FAILED. ebook about attribute does not match etext ID.' )
        dlog( 2, '\tID:', dcmi_ID,'Attribute:', ebookEntry.get ( t_about ) )
        retlog( '%s\t%s\t%s\t%s\t%s' % (  'corruptRDF', proxyItemID, localFileName, iurl, '1' ) )              
        return None
    
    # other types include 'Sound', 'Image', 
    ebookType = ebookEntry.find ( nsdcterms ( 'type') )
    ebookTypeValue =  ebookType.find( nsrdf ( 'Description') ).findtext ( t_value )
    if ebookTypeValue != 'Text':
        dlog( 2, '\tSKIPPING ITEM: non-text type (', ebookTypeValue, ')' )
        return None

    ebookTitle = ebookEntry.findtext ( nsdcterms( 'title' ) )
    ebookTitle = sanitizeString ( ebookTitle )
    if ebookTitle is None:
        dlog( 2, 'ABORT. No title found for item!' )
        dlogAppend( 2, '\n' )
        printDict (hd)
        return
        
    archiveItemID = generateItemID (ebookTitle, etextID)
    
    neverUpdateItems = False
    if neverUpdateItems is True:
        if archiveID_exists( archiveItemID ):
            kdx = 0
            foundFree = False
            while not foundFree:
                archiveItemID = generateItemID (ebookTitle, etextID) + str(kdx).zfill(2)
                foundFree = not archiveID_exists( archiveItemID )
                kdx = kdx + 1
                if kdx > 100:
                    dlog( 2, 'ABORT: Could not find a free archive ID!? Base:',generateItemID (ebookTitle, etextID) )
                    return
       
 #   ebookFormats = ebookEntry.findall ( nsdcterms ( 'hasFormat' ) )
    ebookFormats = et.findall ( nspgterms ( 'file' ) )

    locnames = {}
    
    dlog( 1, 'Compiling file list by inspecting available formats...' )
    for filEl in ebookFormats:
        furi = filEl.get( t_about )
#        furi = fmtEl.get( t_resource )
        # for most files, tail of URI is == local file name
        localFileName = furi.split("/")[-1]
        dlog( 2, "\tURI: %s" % furi ) 
        dlog( 2, '\t\t filesize: %s' % filEl.findtext( nsdcterms( 'extent' ) ) )
        dlog( 2, '\t\t modified: %s' % filEl.findtext( nsdcterms( 'modified' ) ) )
        crawlDelay()
        httpresp = getHeaders("www.gutenberg.org", furi)
        stat = httpresp.status 
        if stat == 404:
             dlog( 2, '\t\t***'  )
             dlog( 2, '\t\t*** WARNING: 404 returned for %s' % furi )
             dlog( 2, '\t\t***' )
             retlog( '%s\t%s\t%s\t%s\t%s' % ( 'fileGot404', archiveItemID, localFileName, furi, '1') )      
        loc = httpresp.getheader( 'location' )
        cd = httpresp.getheader( 'content-disposition' )
        ct =  httpresp.getheader( 'content-type' )
        foundname = False
        if loc is not None:
            localFileName = loc.split("/")[-1]
            dlog( 2, '\t\tEXTRACTED from location: "%s"' % localFileName )
            foundname = True            
        if not foundname and cd is not None:            
            #dlog( 2, cd )
            ftag = "filename="
            l = cd.rpartition(ftag)
            if ftag in l:
                localFileName = l[l.index(ftag)+1] # pull element in list to right of ftag, presumably the filename
                dlog( 2, '\t\tEXTRACTED from content-disposition: "%s"' % localFileName )
                foundname = True                
        if not foundname and ct is not None:
            if ct in  ( 'image/jpeg', 'image/jpg'):
                localFileName = localFileName + ".jpg"
                dlog( 2, '\t\tMODIFIED based on content-type: "%s"' % localFileName )
            elif ct in  ( 'image/jpeg2000', 'image/jp2'):
                localFileName = localFileName + ".jp2"
                dlog( 2, '\t\tMODIFIED based on content-type: "%s"' % localFileName )

        ext = localFileName.rpartition(".")[-1]
        if ext in ( "html", "htm", "rst"):
            dlog( 2, '\t\t**'  )
            dlog( 2, '\t\t** EXCLUDING type with subdirectories: %s' % localFileName )
            dlog( 2, '\t\t**' )
        elif stat != 404:
            locnames[furi] = localFileName   
        
    dlog( 2, 'Retrieving files not in %s...' % itemDir )
    for iurl,localFileName in locnames.iteritems():
        fullLocalFileName = itemDir + localFileName
        if fileExists( fullLocalFileName ) is False:  
            dlog( 2, 'Retrieving %s as %s' % (iurl,localFileName ) )
            if dryrun is True:
                dlog( 2, '(Skipping actual retrieval, dry run)' )
            else:
                crawlDelay()
                curllist = ['curl', '--header','User-Agent:ximm@archive.org (415) 637-5243 :)','--retry','5','-L','--verbose','-o', fullLocalFileName, iurl]
                p = subprocess.Popen( curllist, stderr = subprocess.STDOUT, stdout = subprocess.PIPE )
                (res, err) = p.communicate()
                exitcode = p.wait()
                if fileExists( fullLocalFileName ) is False:  
                #if exitcode != 0:
                    dlog( 1, 'FAILED: Problem retrieving file for %s (%s)'  % (etextID, localFileName) )
                    dlog (1, 'Returned with exitcode %s and message: %s' % (exitcode, res))
                    dlog (1, 'Error: %s' % err)
                    retlog( '%s\t%s\t%s\t%s\t%s' % ( 'missingFile', archiveItemID, localFileName, iurl, '1') )      
                    return None                
        else:
            dlog( 2, '(Already have %s)' % localFileName )
        filesToPost.append( localFileName )
    
    atuple = ( etextID, archiveItemID, et, filesToPost, itemDir )
    return atuple
    
    
def postGutenbergTextToS3 ( postTuple, forceUpload, dryrun, testcollection):

    etextID = postTuple[0]
    archiveItemID = postTuple[1]
    etextMetadataElementTree = postTuple[2]
    filesToPost = postTuple[3]
    itemDir = postTuple[4]

    lt = datetime.datetime.now()
    dlog( 1, '---- POSTING eText %s at: %s' % (etextID, lt) )
    
    # Prepare headers for iaS3 poke of all files
    # based on iaS3 loadup courtesty Mike McCabe woot
    
    # these will be the metadata for item
    headerDict = defaultHeaderDict( True )
    if testcollection is True:
        headerDict['x-archive-meta02-collection'] = 'test_collection'
    itemSpecificHeadersDict = dcmiToMetaHeaders ( etextID, etextMetadataElementTree )
    headerDict.update( itemSpecificHeadersDict )
       
    fnum = len( filesToPost )
    pnum = 0 # posted
    fidx = 1
    if fnum > 0:
        firstFileName = filesToPost.pop(0)    
        res = postFileToS3 ( archiveItemID, itemDir, firstFileName, headerDict, fidx, fnum, forceUpload, dryrun, testcollection)
        if res is True:
            pnum = pnum + 1
        else:
            dlog( 1, 'FAILURE: Could not create bucket ( %s ) with seed file ( %s)' % ( archiveItemID, firstFileName ) )
            dlog( 1, '\n*** NEED TO RETRY ITEM: %s\n' % (archiveItemID) ) # make sure formatted for easy grepping
            retlog( '%s\t%s\t%s%s\t%s' % ( 'retryItem', archiveItemID, firstFileName, 'local', '1') )
            return 1
        fidx = fidx + 1
    else:
        dlog( 1, 'ABORT: No files retrieved to post for item %s' % itemID )
        return 2
        
    # for all subsequent files, do not make a new bucket
    # TK TODO we should probably allow metadata to update each time for now, we have no way 
    #  of knowing on retries if metadata is current
    # headerDict['x-archive-ignore-preexisting-bucket'] = '0'
    headerDict['x-archive-auto-make-bucket'] = '0' # TK
    
    bucketPath = "%s.s3.us.archive.org" % archiveItemID
    
    if not dryrun:
        dlogAppend( 1, 'Verifying bucket exists' )
        timeToCreate = 0
        bucketExists = s3_path_exists( bucketPath ) 
        while bucketExists is False:
            bucketExists = s3_path_exists( bucketPath ) 
            timeToCreate = timeToCreate + 5
            dlogAppend( 2, '.' )
            time.sleep(5)
        else:
            if timeToCreate > 0:     
                dlog( 1, '\nCreated bucket in %s seconds' % str(timeToCreate) )
            else:
                dlog( 2, '\nBucket verified.' )            
            dlogAppend( 2, '\n' )
    
    dlog( 1, '\nPutting remaining %s files...' % (len(filesToPost)) )
    for aSecondaryFile in filesToPost:
        if fidx == fnum:
            # last file
            headerDict['x-archive-queue-derive'] = '1' 
        res = postFileToS3 ( archiveItemID, itemDir, aSecondaryFile, headerDict, fidx, fnum, forceUpload, dryrun, testcollection)
        if res is True:
            pnum = pnum + 1
        else:
            if fidx == fnum:
                dlog ( 1, '\n*** NO DERIVE, last file failed on PUT!\n' )
        fidx = fidx + 1
        time.sleep(0.1)   # give the interface a break...
    lt = datetime.datetime.now()
    if pnum == fnum:
        dlog( 1, 'SUCCESS: All %s files posted (or found).' % pnum )
        ecode = 0
    else:
        dlog( 1, 'FAILURE: Only %s of %s files posted (or found).' % (pnum, fnum) )    
        ecode = 1
    dlog( 1, '---- FINISHED eText %s at: %s' % (etextID, lt) )
    return ecode
        

def postSkippedFileToS3 ( archiveItemID, pfile, purl, retrycount, queueDerive, forceUpload, dryrun, testcollection ):
    """Post a file to a presumably existing bucket (e.g. that needs to be updated or failed initially)."""
    
    etextID = extractIDfromArchiveItemID ( archiveItemID ) 
    dir = tempDirForEtext ( etextID )
    postfilename = dir + pfile
    
    headerDict = defaultHeaderDict( False )
    if queueDerive is True:
        headerDict['x-archive-queue-derive'] = '1' 
    
    if fileExists( postfilename ) is False:
        if dryrun is True:
            dlog( 1, '\t(Local file missing, but ignoring and spoofing filesize; dry run)' )
            content_length = 12345
        else:
            dlog( 1, '\tABORT: file to send missing: %s' % postfilename )
            retlog( '%s\t%s\t%s\t%s\t%s' % ( 'missingFile', archiveItemID, pfile, purl, (retrycount + 1) ) )            
            return 2
    else:
        content_length = os.path.getsize( postfilename )

    path = "http://s3.us.archive.org/" + archiveItemID + "/" + pfile
        
    if forceUpload is not True:
        if archiveHasCurrentFile( archiveItemID, pfile, content_length):
            dlog( 1, '\tSKIPPING FILE: Already current in Archive' )
            return 0

    dlog( 1, '\tPUT: %s to %s' % (postfilename, path) )

    dlogHead (content_length, headerDict)
        
    if dryrun is True:
        dlog( 1, '\tABORT: Dry run' )
        dlogAppend( 2, '\n' )
        return 0
        
    curllist = ['curl', '--location']
    for k, v in headerDict.iteritems():
        curllist.append( '--header' )
        curllist.append( "%s:%s" % (k.encode('utf-8'), v.encode('utf-8')) )
#    curllist.append( '--write-out' )
#    curllist.append( '%{http_code}' )
    curllist.append( '--retry' )
    curllist.append( '5' )
    curllist.append( '--verbose' )    
    curllist.append( '--upload-file' )
    curllist.append( postfilename )
    curllist.append( path )
    
    # curlcmd = ' '.join([str(v) for v in curllist])

    # post the file using curl, use of --write-out returns the HTTP response code
    p = subprocess.Popen( curllist, stderr = subprocess.STDOUT, stdout = subprocess.PIPE )
    (res, err) = p.communicate()
    exitcode = p.wait()
        
    if exitcode == 0:
        dlog( 1, '\tSuccess!' ) 
        return 0
    else:
        dlog( 1, '\tFailed! (Exit code %s)' % exitcode)
        dlog( 1, '\tError: %s' % err)
        dlog( 1, '\tResponse:\n %s' % res)
        dlog( 1, repr(curllist) )
        dlog( 1, '\n*** NEED TO RETRY FILE: %s %s\n' % (archiveItemID, pfile) ) # make sure formatted for easy grepping
        retlog( '%s\t%s\t%s\t%s\t%s' % ( 'retryFile', archiveItemID, pfile, purl, (retrycount + 1) ) )
        return 1

           
   
def postFileToS3 ( archiveItemID, dir, pfile, headerDictionary, fidx, fnum, forceUpload, dryrun, testcollection=True ):
    """Return True if posted or already present, False on error conditions that were not recovered"""

    postfilename = dir + pfile
    
    if fileExists( postfilename ) is False:
        if dryrun is True:
            dlog( 1, '(Local file missing, but ignoring and spoofing filesize; dry run)' )
            content_length = 12345
        else:
            dlog( 1, 'ABORT: file to send missing: %s' % postfilename )
            retlog( '%s\t%s\t%s\t%s\t%s' % ( 'missingFile', archiveItemID, pfile, 'null', '1') )
            return False
    else:
        content_length = os.path.getsize( postfilename )

    path = "http://s3.us.archive.org/" + archiveItemID + "/" + pfile
    
    dlog( 1, '[%s of %s] PUT: %s to %s' % (fidx,fnum, postfilename, path) )
    
    if forceUpload is not True:
        if archiveHasCurrentFile( archiveItemID, pfile, content_length):
            dlog( 1, 'SKIPPING FILE: Already current in Archive' )
            dlogAppend( 2, '\n' )
            return True

    dlogHead (content_length, headerDictionary)
        
    if dryrun is True:
        dlog( 1, 'ABORT: Dry run' )
        dlogAppend( 2, '\n' )
        return True
        
    curllist = ['curl', '--location']
    for k, v in headerDictionary.iteritems():
        curllist.append( '--header' )
        curllist.append( "%s:%s" % (k.encode('utf-8'), v.encode('utf-8')) )
#    curllist.append( '--write-out' )
#    curllist.append( '%{http_code}' )
    curllist.append( '--retry' )
    curllist.append( '5' )
    curllist.append( '--verbose' )    
    curllist.append( '--upload-file' )
    curllist.append( postfilename )
    curllist.append( path )
    
    # curlcmd = ' '.join([str(v) for v in curllist])

    # post the file using curl, use of --write-out returns the HTTP response code
    p = subprocess.Popen( curllist, stderr = subprocess.STDOUT, stdout = subprocess.PIPE )
    (res, err) = p.communicate()
    exitcode = p.wait()
        
    if exitcode == 0:
        dlog( 1, '\tSuccess!' ) 
        return True
    elif exitcode == 52:
        dlog( 1, '\tEmpty reply from server!?' )
        time.sleep(5)
        if archiveHasCurrentFile( archiveItemID, pfile, content_length) is False:
            dlog( 1, '\tPossible error: %s' % err)
            dlog( 1, '\tResponse:\n %s' % res)
            dlog( 1, repr(curllist) )
            dlog( 1, '\n*** NEED TO RETRY FILE (Empty Response): %s %s\n' % (archiveItemID, pfile) ) # make sure formatted for easy grepping
            retlog( '%s\t%s\t%s\t%s\t%s' % ( 'retryFileEmptyResponse', archiveItemID, pfile, "local", '1') )
            return False
        else:
            dlog( 1, '\t*** BAD SERVER RESPONSE (Exit code %s) but file posted OK.' % exitcode)
            dlog( 1, '\tError: %s' % err)
            dlog( 1, '\tResponse:\n %s' % res)
            dlog( 1, repr(curllist) )
            return True         
    else:
        time.sleep(2)
        if archiveHasCurrentFile( archiveItemID, pfile, content_length) is False:
            dlog( 1, '\tFailed! (Exit code %s)' % exitcode)
            dlog( 1, '\tError: %s' % err)
            dlog( 1, '\tResponse:\n %s' % res)
            dlog( 1, repr(curllist) )
            dlog( 1, '\n*** NEED TO RETRY FILE: %s %s\n' % (archiveItemID, pfile) ) # make sure formatted for easy grepping
            retlog( '%s\t%s\t%s\t%s\t%s' % ( 'retryFile', archiveItemID, pfile, "local", '1') )
            return False
        else:
            dlog( 1, '\t*** BAD SERVER RESPONSE (Exit code %s) but file posted OK.' % exitcode)
            dlog( 1, '\tError: %s' % err)
            dlog( 1, '\tResponse:\n %s' % res)
            dlog( 1, repr(curllist) )
            return True 

# TK assumes the item is searchable and already indexed
def archiveItem_exists ( gutenbergID ):
    """Check whether there is an Archive item already for a given Gutenberg eText. Fails if item exists but is non-searchable!"""
    searchPath = "http://www.archive.org/advancedsearch.php?q=call_number%3Agutenberg%3Fetext%23%3F" + gutenbergID + "&fl%5B%5D=call_number&output=xml"
    rt = etree.parse ( searchPath ) 
    if rt is not None:
        resultEl = rt.find ('result')
        if resultEl.get('numFound') != '0':
            return True
    return False

# TK assumes the item is searchable and already indexed
def archiveID_exists ( archiveID ):
    """Check whether there is an Archive item already for a given Archive ID."""
    itemInfoPath = "http://www.archive.org/services/find_file.php?file=" + archiveID
#    searchPath = "http://www.archive.org/advancedsearch.php?q=identifier%3A" + archiveID + "&fl%5B%5D=identifier&output=xml"
    rt = etree.parse ( itemInfoPath ) 
    if rt is not None:
        mel = rt.find ('metadata')
        if mel is not None:
            return mel.findtext('identifier') == archiveID
    return False

def archiveID_files ( archiveID ):
    """Return as a list of lxlml elements all files that currently exists for a given Archive ID."""
    dlog( 3, 'Checking for files for %s' % archiveID )
    itemInfoPath = "http://www.archive.org/services/find_file.php?file=" + archiveID
#    searchPath = "http://www.archive.org/advancedsearch.php?q=identifier%3A" + archiveID + "&fl%5B%5D=identifier&output=xml"
    rt = etree.parse ( itemInfoPath ) 
    if rt is not None:
        flist = rt.find ('files')
        if flist is not None:
            dlog( 4, '\t%s' %flist.findall('file') )
            return flist.findall('file')
    return []    

def archiveHasCurrentFile ( archiveID, fname, fsize ):
    """Answer whether the Archive has a [current] version of fname, based on size alone(!)"""
    filesRemote = archiveID_files( archiveID )
    dlog( 3, 'Checking %s for %s (with size %s)' % (archiveID, fname, fsize) )
    if len(filesRemote) == 0:
        return False
    matches = [f for f in filesRemote if f.get('name') == fname]
    if len(matches) == 0:
        return False
    if fsize != -1:
        return matches[0].findtext( 'size' ) == str(fsize)
    else:
        return True

# original formulation courtesy Mike McCabe
def s3_path_exists( path ):
    try:
        conn = httplib.HTTPConnection( path )
        conn.request('HEAD', '/')
        res = conn.getresponse() 
        # dlog( 3, 'HTTP HEAD response: %s' % res.status)
        redirectResponses = [301, 302, 307]
        goodResponses = [200, 500]
        if res.status in redirectResponses:
            loc = res.getheader('location')
            lp = urlparse(loc)
            nlp = lp.netloc.split(':')
            host = nlp[0]
            port = lp.port
            if len(nlp) > 1:
                port = nlp[1]
            conn = httplib.HTTPConnection(host, port=port)
            # TK TODO need to use GET here as HEAD is failing after the redirect with a 500 Server Error
            # there appears to be different code version running on the iaxxxxx machines that the redirect points to
            conn.request('GET', '/')
            res = conn.getresponse()
        if res.status in goodResponses:
            return True
        else:
            return False
    except httplib.BadStatusLine, e:
        etime = datetime.datetime.now()
        dlog( 2, 'ERROR: exists-check BadStatusLine %s %s %s' % (e, path, etime) )
    return False

def generateItemID (title, idnum):
    title = printable( title )
    title = "".join(i for i in title if i.isalpha())
    title = title.lower()
    title = title.replace(' ','')
    if len(title) == 0:
        title = 'pgcommunitytexts'
    title = title[0:16]
    id = title + idnum.zfill(5) + 'gut'   
#    id = title + idnum + 'gut'
    return id
  
def extractIDfromArchiveItemID ( aid ):
    l = len(aid)
    if l > 7:
        return aid[ (l-8):(l-3) ] 
    else:
        # FAIL
        return "00000"
  
def tempDirForEtext ( etid ):
    itemDir = './temp_' + etid + '/'
    if os.access(itemDir, os.F_OK) is False:
        os.mkdir(itemDir)
        dlog( 2, 'Making item directory %s' % itemDir )
    else:
        dlog( 2, 'Found existing item directory %s' % itemDir )
    return itemDir

def removeItemDir( itemDir ):
    if os.access(itemDir, os.F_OK) is True:
        dlog( 1, 'Cleanup: removing item directory %s' % itemDir )
        shutil.rmtree( itemDir )

def fileExists ( path ):
    return os.access( path, os.F_OK)

def defaultHeaderDict ( makeBucket=True ):
    # following are for ximm@archive.org
    accesskey = "jGtgXt7sQMGnWmMf"
    secret = "I936OiX9uQVuc04d"
    hd = {
            'x-archive-auto-make-bucket': '1',
            'x-archive-ignore-preexisting-bucket': '1',
            'x-archive-meta01-collection': 'gutenberg',
            'x-archive-meta-contributor': 'Project Gutenberg',
            'x-archive-meta-mediatype': 'texts',
#            'x-archive-meta-noindex': 'false',    # make searchable in search engine
            'x-archive-queue-derive': '0', 
            'authorization': "LOW %s:%s" % (accesskey, secret) }
    if not makeBucket:
            hd['x-archive-ignore-preexisting-bucket'] = '0'
            hd['x-archive-auto-make-bucket'] = '0' # TK
    return hd

def dcmiToMetaHeaders ( etextID, et ):

    hd = {}
        
    t_resource = nsrdf ( 'resource')
    t_about = nsrdf ( 'about')
    t_value = nsrdf ( 'value')

    ebookEntry = et.find( nspgterms ('ebook') )
    # Scrape item-specific meta data from ebookEntry, encode it as headers.

    # provide a pointer back to GP :)
    hd ['x-archive-meta-source'] = 'http://www.gutenberg.org/ebooks/' + etextID

    # TK GP also has a 'friendly title' which seems often to be length truncated and/or use the form 'X by Y'
    ebookTitle = ebookEntry.findtext ( nsdcterms( 'title' ) )
    ebookTitle = sanitizeString ( ebookTitle )
    hd ['x-archive-meta-title'] = ebookTitle

    # multiple language entries are stored in a Bag collection with each item indexed by 'li' :P
    # order doesn't matter
    langElement = ebookEntry.find ( nsdcterms ('language' ) )
    langBag = langElement.findall ( nsrdf ('Bag') )
    if len(langBag) > 0:
        langEls = langBag[0].findall ( nsrdf ( 'li' ) )
        idx = 1
        for aLangEl in langEls:
            sidx = str(idx).zfill(2)
            ebookLanguage = aLangEl.text
            hd ['x-archive-meta' + sidx + '-language'] = iso639_2toIso639_3 (ebookLanguage)
            idx = idx + 1
    else:        
        ebookLanguage = ebookEntry.findtext ( nsdcterms( 'language' ) )
        hd ['x-archive-meta-language'] = iso639_2toIso639_3 (ebookLanguage)
    
    #TK some existing GP texts in IA were entered with 'numeric_id'
    ebookCallNumber = 'gutenberg etext# ' + etextID
    hd ['x-archive-meta-call--number'] = ebookCallNumber
    
    # what we call a creator (author) is a reference pointer to elsewhere in the tree
    # there can be 0-N contributors, each identified in the file's RDF with a MARC relator code
    # we prilege the first author found, since our creator list is not indexed; all we control is ordering
    # set up idx to be the index used for S3 metatags for any subsequent creator entries
    agentsList = et.findall( nspgterms ('agent') )
    ebookCreatorRefEl = ebookEntry.find ( nsdcterms( 'creator' ) )
    idx = 1
    if ebookCreatorRefEl is not None:
        ebookCreatorRef = ebookCreatorRefEl.get ( t_resource )
        matches = [age for age in agentsList if age.get( t_about ) == ebookCreatorRef]
        if len(matches) != 0:
            creatorEl = matches[0]    
            creatorName = creatorEl.findtext( nspgterms( 'name' ) )
            creatorBirthdate = creatorEl.findtext( nspgterms( 'birthdate' ) )
#            if creatorBirthdate != None:
#                hd['x-archive-meta01-creator-birthdate'] = creatorBirthdate
            creatorDeathdate = creatorEl.findtext( nspgterms( 'deathdate' ) )
#            if creatorBirthdate != None:
#                hd['x-archive-meta01-creator-deathdate'] = creatorDeathdate
            if creatorBirthdate != None:
                if creatorDeathdate != None:
                    creatorName = creatorName + ', ' + creatorBirthdate + '-' + creatorDeathdate
                else:
                    creatorName = creatorName + ', ' + creatorBirthdate + '-'       
            hd['x-archive-meta01-creator'] = creatorName
            creatorAliases = creatorEl.findall( nspgterms( 'alias' ) )
            for ale in creatorAliases:
                # these will be in arbitrary order in our meta data, which is OK
                # TK tag name?
                hd['x-archive-meta01-creator-alias'] = ale.text
            idx = 2    

    # TK
    # map what PG calls 'contributors' (MARC relators) to our creator field
    # this list covers of all types found at time or writing in the current PG catalog
    # a comprehensive mapping could be constructed from the listing at:
    #   http://id.loc.gov/vocabulary/relators.html
    # Currently rendering them on to the end of the name, following style adopted in PG catalog.rdf [in old DCMI]
    contributorTagDict = { 
                           nsmarcrel ( 'adp' ) : '[Adapter]',
                           nsmarcrel ( 'ann' ) : '[Annotator]',
                           nsmarcrel ( 'art' ) : '[Artist]',
                           nsmarcrel ( 'aui' ) : '[Author of introduction, etc.]',
                           nsmarcrel ( 'cmm' ) : '[Commentator]',
                           nsmarcrel ( 'com' ) : '[Compiler]',
                           nsmarcrel ( 'ctb' ) : '[Contributor]',
                           nsmarcrel ( 'edt' ) : '[Editor]',
                           nsmarcrel ( 'egr' ) : '[Engraver]',
                           nsmarcrel ( 'ill' ) : '[Illustrator]',
                           nsmarcrel ( 'oth' ) : '[Other]',
                           nsmarcrel ( 'prf' ) : '[Performer]',
                           nsmarcrel ( 'pht' ) : '[Photographer]',
                           nsmarcrel ( 'prt' ) : '[Printer]',
                           nsmarcrel ( 'pbl' ) : '[Publisher]',
                           nsmarcrel ( 'res' ) : '[Researcher]',
                           nsmarcrel ( 'trc' ) : '[Transcriber]',
                           nsmarcrel ( 'trl' ) : '[Translator]',
                           nsmarcrel ( 'unk' ) : '[Unknown Role]' }



    # TODO this is actually backwards, we should work from each contributor to identifying marcrel role
    #  that will be particularly important when we have a table translating notation/code to human readable term
    for contributorTag, contributorText in contributorTagDict.iteritems():    
        ebookContributorList = ebookEntry.findall( contributorTag )
        for ebookContributorRefEl in ebookContributorList:
            if ebookContributorRefEl is not None:
                ebookContributorRef = ebookContributorRefEl.get ( t_resource )
                matches = [age for age in agentsList if age.get( t_about ) == ebookContributorRef]
            else:
                matches = []
            for contEl in matches:
                sidx = str(idx).zfill(2)
                contName = contEl.findtext( nspgterms( 'name' ) )
                contBirthdate = contEl.findtext( nspgterms( 'birthdate' ) )
#                if contBirthdate != None:
#                    hd['x-archive-meta' + sidx + '-creator-birthdate'] = contBirthdate
                contDeathdate = contEl.findtext( nspgterms( 'deathdate' ) )
#                if contBirthdate != None:
#                    hd['x-archive-meta' + sidx + '-creator-deathdate'] = contDeathdate
                if contBirthdate != None:
                    contName = contName + ', ' + contBirthdate + '-' 
                    if contDeathdate != None:
                        contName = contName + contDeathdate        
                contName = contName + ' '  + contributorText    
                hd['x-archive-meta' + sidx + '-creator'] = contName
                contAliases = contEl.findall( nspgterms( 'alias' ) )
                for ale in contAliases:
                    # these will be in arbitrary order in our meta data, which is OK
                    # TK tag name?
                    hd['x-archive-meta' + sidx + '-creator-alias'] = ale.text
                idx = idx + 1
    # TK
    # Agents (and subjects etc.?) can have reference materials linked:
    # These appear as siblings of the Agent (and eBook) elements... :O
    #     <rdf:Description rdf:about="http://en.wikipedia.org/wiki/Andrew_Lang">
    #       <dcterms:description>en.wikipedia</dcterms:description>
    #     </rdf:Description>
    # We are currently losing this information.

    ebookLicense = et.find( nsccterms( 'Work' ))
    if ebookLicense is not None:
        licEl = ebookLicense.find( nsccterms( 'license' ))
        if licEl is not None:
            licurl = licEl.get( t_resource )
            if licurl is not None:
                hd['x-archive-meta-licenseurl'] = licurl
            
    ebookRights = ebookEntry.find( nsdcterms( 'rights' ))
    if ebookRights is not None:
        hd['x-archive-meta-rights'] = ebookRights.text
        
    # separate subject entries for type LCC, Library of Congress Collection type descriptor
    lccList = []
    ebookSubjectList = ebookEntry.findall ( nsdcterms( 'subject' ) )
    idx = 0
    lccString = NSMap['dcterms'] + 'LCC'
    for aSubEl in ebookSubjectList:
        aDescEl = aSubEl.find( nsrdf( 'Description' ) )
        mo = aDescEl.find( nsdcam( 'memberOf' ) )
        if mo.get( t_resource ) == lccString:
            vals = aDescEl.findall( t_value )
            for v in vals:
                lccList.append(v.text)
            continue
        # LCSH entries   
        vals = aDescEl.findall( t_value )
        for v in vals:
            if idx > 0:
                sidx = str(idx).zfill(2)
                hd['x-archive-meta' + sidx + '-subject'] = v.text
            else:
                hd['x-archive-meta-subject'] = v.text
            idx = idx + 1
    
    # TK Various GP texts have different delimiters: ' -- ' being most common, also ' - ', also '\n\n'
    ebookTOC = ebookEntry.find( nsdcterms( 'tableOfContents' ))
    if ebookTOC is not None:
        # make sure there are no newlines in the text we scrape; these break iaS3
        llist = ebookTOC.text.splitlines()
        toc = llist.pop(0)
        for aline in llist:
            toc = toc + ' -- ' + aline.strip()            
        hd['x-archive-meta-table--of--contents'] = toc
    
    # construct our description to match existing items
    ourDesc = 'Book from Project Gutenberg: %s' % ebookTitle
    if len(lccList) > 0:
        if len(lccList) == 1:
            ourDesc = ourDesc + ' Library of Congress Classification: ' + lccList[0]
        else:
            ourDesc = ourDesc + ' Library of Congress Classifications: ' + lccList.pop(0)
            for anLCC in lccList:
                ourDesc = ourDesc + ', ' + anLCC            
    ebookAlternative = ebookEntry.find( nsdcterms( 'alternative' ))
    # alternative is used as an AKA for titles (e.g. 'Arabian Nights' for 'The book of 1001...'; or a translation)
    if ebookAlternative is not None:
        ourDesc = ourDesc + ' Note: ' + ebookAlternative.text        
    ebookDescription = ebookEntry.find( nsdcterms( 'description' ))
    if ebookDescription is not None:
        # make sure there are no newlines in the text we scrape; these break iaS3
        dlist = ebookDescription.text.splitlines()
        desc = dlist.pop(0)
        for aline in dlist:
            desc = desc + ' ' + aline.strip()
        ourDesc = ourDesc + ' Note: ' + desc 
    hd['x-archive-meta-description'] = ourDesc    

    #last-minute sanitization, wash out newlines
    for k, v in hd.iteritems():
        if "\n" in v:
            vlist = v.splitlines()
            newv = vlist.pop(0)
            for aline in vlist:
                newv = newv + ' ' + aline.strip()
            hd[k] = newv
    
    return hd
  
def getHeaders (dom, path):
    """Use the HTTP HEAD command to get only the headers for path. Return the HTTPResponse object"""
    conn = httplib.HTTPConnection( dom )
    conn.request( 'HEAD', path )
    return conn.getresponse()  
  

# Namespace convenience accessors    
def nsr (ns, e):
    global NSMap
    return '{%s}%s' % (NSMap[ns], e)  
def nsdcterms (e):
    global NSMap
    return '{%s}%s' % (NSMap['dcterms'], e)  
def nsrdf (e):
    global NSMap
    return '{%s}%s' % (NSMap['rdf'], e)  
def nspgterms (e):
    global NSMap
    return '{%s}%s' % (NSMap['pgterms'], e)  
def nsmarcrel (e):
    global NSMap
    return '{%s}%s' % (NSMap['marcrel'], e)  
def nsdcam (e):
    global NSMap
    return '{%s}%s' % (NSMap['dcam'], e)       
def nsccterms (e):
    global NSMap
    return '{%s}%s' % (NSMap['cc'], e)      
  
def buildLanguageDictionary():
    global langDict
    langDict = {}
    isoMappingFile = 'ISO-639-2_utf-8.txt'
    if not fileExists( isoMappingFile ):
        dlog( 2, 'ERROR: No language mapping file. Expected', isoMappingFile )
    mapFile = open(isoMappingFile, "r")
    line = mapFile.readline()
    while line:
        linelist = line.rsplit('|')
        langDict[ linelist[2] ] = linelist[0]
        line = mapFile.readline()
    mapFile.close() 

def iso639_2toIso639_3 ( iso2code ):
    global langDict
    try:
        langDict
    except NameError:
        buildLanguageDictionary()
    if iso2code in langDict:
        return langDict[iso2code]
    else:
        return iso2code

def crawlDelay():
    """Gutenberg robots.txt asks for Crawl-delay of 10 seconds currenlty..."""
    cd = 10.5
    dlog( 2, '\tCRAWL-DELAY: %s seconds' % cd ) ) )
    time.sleep(cd)
    return

# Logging   
            
def dlog ( lev, str ):
    global dlogfile
    global dloglevel
    if lev <= dloglevel and dlogfile is not None:
        lt = datetime.datetime.now()
        try:
            dlogfile.write( "%s %s\n" %  ( lt,  str.encode( 'utf-8' ) ) )
        except:
            try:
                dlogfile.write ( "%s <* removed unprintable chars *> %s\n" % (lt, printable( str ) ) )
            except:
                dlogfile.write( "%s <* unprintable *>\n" % lt )
        # print str
    return

def dlogAppend ( lev, str ):
    global dlogfile
    global dloglevel
    if lev <= dloglevel and dlogfile is not None:
        try:
            dlogfile.write( "%s" %  str.encode( 'utf-8' ) )
        except:
            try:
                dlogfile.write ( "<* removed unprintable chars *> %s" % printable( str ) )
            except:
                dlogfile.write( "<* unprintable *>\n" )
        # print str
    return

def dlogHead (siz, aDict):
    dlog( 2, "IAS3 HTTP headers:" )
    dlog( 2, '\tcontent-length = %s' % siz )
    for k,v in aDict.iteritems():    
        dlog( 2, '\t%s = %s' % (k,v) )

def retlog ( str ):
    global retlogfile
    try:
        retlogfile.write( "%s\n" %  str.encode( 'utf-8' ))
    except:
        try:
            retlogfile.write ( "<* removed unprintable chars *> %s\n" % printable( str ) )
        except:
            retlogfile.write( "<* unprintable *>\n" )
    # print str
    return


def printable ( dirty ):
    # eliminate non-printable chars
    clean = "".join(i for i in dirty if ord(i) < 128)
#    clean = ''.join([char for char in dirty if isascii(char)])
#    return ''.join([char for char in clean if isprint(char)])
    return clean

def sanitizeString ( dirty ):
    # eliminate only tabs and newlines
    clean = string.replace( dirty, "\n", " ") 
    clean = string.replace( clean, "\r", " ") 
    clean = string.replace( clean, "\t", "    ") 
#    clean = ''.join(char for char in dirty if (char not in ["\n","\t","\r"]))
    return clean
    
# Remember the Main

def main(argv=None):

    global verbose
    global dlogfile         # [verbose] logging for gp2ia
    global retlogfile       # retry log in CSV form
    global dloglevel        # for our own logging only; 1 = terse, 2 = verbose, 3 = debugging
    
    etextNumber = "00000"
    
    if argv is None:
        argv = sys.argv
        
    forceUpload = False
    dryrun = False
    verbose = False    
    retry = False
    testcollection = True
    cleanup = False
    
    logdir = "./gplogs/"
    dlfn = None
    
    logmode = "a"
    
    for anArg in argv:
        if anArg[0] is "-":
            qual = anArg[1:len(anArg)]
            if qual == "dry":
                dryrun = True
            elif qual == "verbose":
                verbose = True
            elif qual == "force":
                forceUpload = True
            elif qual == "cleanup":
                cleanup = True
            elif "retry=" in qual:
                retrylogfn = qual.split("retry=")[-1]
                retry = True
            elif qual == "live":
                testcollection = False
            elif "log=" in qual:
                dlfn = logdir + qual.split("log=")[-1]
                logmode = "a"
        else:
            try:
                num = str(int(anArg))
            except:
                # anArg is not all numeric, probably my file na
                pass
            else:    
                etextNumber = anArg

    rlfn = logdir + '0000_retry.tsv'     # outgoing log of files that need to be retried

    if dlfn is None:
        if retry is True:
            dlfn = logdir + 'retry_on_' + retrylogfn + '.log'       # by definition should be .log
            if len(retrylogfn) > 3:
                rnum = int(retrylogfn[0:4]) + 1
            else:
                rnum = 0
            rlfn = logdir + str(rnum).zfill(4) + '_retry.tsv'  # outgoing log of files that need to be retried AGAIN
        else:
            dlfn = logdir + etextNumber + '.log'
    
    if verbose is True:
        dloglevel = 2
    else:
        dloglevel = 1
        
    with codecs.open( dlfn, encoding='utf-8', mode=logmode ) as dlogfile:
        with codecs.open( rlfn, encoding='utf-8', mode=logmode ) as retlogfile:
            if retry is True:
            #
            # Retrying
            #
            # Currently no support for updating metadata, we assume failed items are handled by
            #  being re-submitted, and metadata is always populated during fetch/post-cum-bucket creation
            # Retry is only for the case of files missing from otherwise existing buckets
                dlog( 1, "\n\n**** RETRYING %s ****\n\n" % retrylogfn)
                rfn = logdir + retrylogfn
                retrylogfile = open( rfn, "r")
                ecode = 0
                ridx = 1
                for aline in retrylogfile:
                    litems = aline.split()
                    # print litems
                    if len(litems) > 3:
                        rtype = litems[0]  
                        archiveItemID = litems[1]
                        retryfile = litems[2]
                        rurl = litems[3]
                        retrycount = int(litems[4])
                        if rtype == "retryFile" or rtype == "retryFileEmptyResponse":
                            queueDerive = True
                            res = postSkippedFileToS3 ( archiveItemID, retryfile, rurl, retrycount, queueDerive, forceUpload, dryrun, testcollection )
                            if res == 0:
                                # TODO cleanup if that was the last file...? How to know... :P
                                dlog(1, "gp2ia [RETRY %s %s]: (0) Posted [ %s in %s]" % ( retrylogfn, ridx, archiveItemID, retryfile ))
                            elif res == 2:
                                dlog(0, "gp2ia [RETRY %s %s]: (1) couldn't find expected data to post [ %s :: %s ]" % ( retrylogfn, ridx, archiveItemID, retryfile ))
                                ecode = 2
                            elif res == 1:
                                dlog(0, "gp2ia [RETRY %s %s]: (1) failed trying to post file to IAS3 interface [ %s :: %s ]" % (  retrylogfn, ridx, archiveItemID, retryfile ))
                                ecode = 1
                            else:
                                dlog(0, "gp2ia [RETRY %s %s]: (1) unknown failure posting [ %s :: %s ]" % ( retrylogfn, ridx, archiveItemID, retryfile ))
                                ecode = 1
                        elif rtype == "retryItem" or rtype == "missingRDF":
                            etextNumber = extractIDfromArchiveItemID ( archiveItemID )
                            itemTuple = fetchGutenbergText ( etextNumber, True, dryrun ) # forceUpload = True
                            if itemTuple is None:
                                dlog(0, 'gp2ia [RETRY %s %s]: (1) failed to retrieve item data for %s, skipping (expected if call number exists in Archive)' % ( retrylogfn, ridx, archiveItemID ))
                                ecode = 1
                            else:
                                res = postGutenbergTextToS3 ( itemTuple, forceUpload, dryrun, testcollection )
                                if res == 0:
                                    if cleanup is True and dryrun is False:
                                        removeItemDir( itemDir )   
                        elif rtype == "fileGot404":
                                # TK TODO
                                # missing code: try again to retrieve and post file X for existing item
                                # for now, just force the whole item to update: existing files will be skipped
                                dlog(0, "gp2ia [RETRY %s %s]: (1) couldn't find expected data to post [ %s :: %s ]" % ( retrylogfn, ridx, archiveItemID, retryfile ))
                                retlog( "%s\t%s\t%s\t%s\t%s" % (rtype, archiveItemID, retryfile, rurl, retrycount) )
                        elif rtype == "missingFile":
                            if rurl == "null":
                                dlog(0, "gp2ia [RETRY %s %s]: (1) couldn't find expected data to post [ %s :: %s ]" % ( retrylogfn, ridx, archiveItemID, retryfile ))
                                retlog( "%s\t%s\t%s\t%s\t%s" % (rtype, archiveItemID, retryfile, rurl, retrycount) )
                            else:
                                # TK TODO
                                # missing code: retrieve and post file X for existing item
                                # for now, just force the whole item to update: existing files will be skipped
#                                dlog(0, "gp2ia [RETRY %s %s]: (1) CODE NOT YET WRITTEN TO RETRIEVE AND POST [ %s :: %s ]" % ( retrylogfn, ridx, archiveItemID, retryfile ))                            
#                                retlog( "%s\t%s\t%s\t%s\t%s" % (rtype, archiveItemID, retryfile, rurl, retrycount) )
                                etextNumber = extractIDfromArchiveItemID ( archiveItemID )
                                itemTuple = fetchGutenbergText ( etextNumber, True, dryrun ) # forceUpload = True
                                if itemTuple is None:
                                    dlog(0, 'gp2ia [RETRY %s %s]: (1) failed to retrieve item data for %s, skipping (expected if call number exists in Archive)' % ( retrylogfn, ridx, archiveItemID ))
                                    ecode = 1
                                else:
                                    res = postGutenbergTextToS3 ( itemTuple, forceUpload, dryrun, testcollection )
                    ridx = ridx + 1
                return ecode

            else:
            #
            # Normal retrieve-post cycle                
            #
                if etextNumber is None:
                    dlog(0, 'gp2ia: (2) no etext specified, aborting')
                    return 2
                itemDir = tempDirForEtext( etextNumber )                    
                itemTuple = fetchGutenbergText ( etextNumber, forceUpload, dryrun ) # returns none on Fails
                if itemTuple is None:
                    dlog(0, 'gp2ia: (1) failed to retrieve item data for %s, skipping (expected if call number exists in Archiv[e)' % etextNumber)
                    return 1
                else:
                    res = postGutenbergTextToS3 ( itemTuple, forceUpload, dryrun, testcollection )
                    if res == 0:
                        if cleanup is True and dryrun is False:
                            removeItemDir( itemDir )
                        dlog(1, "gp2ia: (0) posted %s" % etextNumber)
                        return 0
                    elif res == 2:
                        dlog(0, "gp2ia: (1) couldn't find expected data to post for %s" % etextNumber)
                        return 1
                    elif res == 1:
                        dlog(0, "gp2ia: (1) failed trying to post file to IAS3 interface for %s" % etextNumber)
                        return 1
                    else:
                        dlog(0, "gp2ia: (1) unknown failure posting %s" % etextNumber)
                        return 1
                        
if __name__ == "__main__":
    sys.exit(main())
