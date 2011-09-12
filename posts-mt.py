#!/usr/bin/env python

import socks
import socket
import time
import re
import os
import sys
from multiprocessing import Process, Queue
from Queue import Empty, Full
import redis
from config import username, password
from twill.commands import *

"""
posts-mt.py

A multithreaded crawler for the offtopic.com forum.
Uses Redis for parallelism.
"""

#Remove this in EC2

#Uncomment the lines below to use Tor (if installed), if worried about
#anonymity:
#socks.setdefaultproxy(socks.PROXY_TYPE_SOCKS4, '127.0.0.1', 9050, True)
#socket.socket = socks.socksocket


"""
Redis record type for each piece of work:
post id:page no
-----------------
(second page of post 174408)
174408:2
"""


sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 0)

def login():
    b = get_browser()
    try:
      b.go("http://forums.offtopic.com")
      fv("1", "vb_login_username", username)
      fv("1", "vb_login_password", password)
      b.submit('4')
      b.go("http://forums.offtopic.com")
    except:
      return b
    return b

def main():
    """
    main

    Just another main function. Requires input from the user:
    1st arg: number of cores to use. With HT, enter 2*cores
    2nd arg: master IP where Redis runs.
    """
    #Cores must be provided by user.
    cores = int(sys.argv[1])
    #Master IP is the instance where the Redis work queue runs.
    try:
        master_ip = sys.argv[2]
    except IndexError:
        master_ip = None
    start = 0
    end = 4760000
    if master_ip != None:
        print "Using Redis at %s." % master_ip
    else:  #change this to be "first time" run
        r = redis.Redis()
        r.flushall()
        print "Done flushing database."
        #Tell the work queue that we want to crawl the first page of every post.
        for x in xrange(i, end+1):
            r.rpush('postids', str(x) + ":1")
        print "Done inserting to Redis queue."
    procs = [Process(target=worker, args=(master_ip,)) for p in range(cores)]
    for p in procs:
        p.start()
    for p in procs:
        p.join()

def worker(master):
    b = login()
    if master:
        r = redis.Redis(master, 6379)
    else:
        r = redis.Redis()
    error = "No Thread specified" #In HTML if post does not exist.
    #Create a manifest that records, for each post ID, if it exists.
    EXISTENCE = open("posts-%s.log" % str(os.getpid()), "a")
    while True:
        i = r.lpop('postids')
        if not i:
            break #DONE
        try:
            post, page = i.split(":")
        except:
            print "[WARN] Did not contain post/page tuple. Skipping..."
            continue
        try:
            b.go("http://forums.offtopic.com/showthread.php?t=%d&page=%d" % (int(post), int(page)))
        except:
            print "[WARN] Couldn't retrieve post id %s page %s. Reinserting..." % (post, page)
            r.lpush('postids', i)
            continue #possible loss of data here.
        #Test for redirect.
        retr_url = b.get_url()
        if str(post_re.search(retr_url).group(1)) != post:
            print "[NOTE] Post %s was redirected to %s." % (post, retr_url)
            red_post = post_re.search(retr_url).group(1).strip()
            r.lpush('postids', red_post + ":" + page)
            continue
        temp = b.get_html()
        if "You are not logged in" in temp:
            print "[WARN] Logged out. Logging back in..."
            b = login()
            r.lpush('postids', i)
            continue 
        else:
            print >> EXISTENCE, '\t'.join([str(i), str(error not in temp)])
            if error not in temp:
                if page == "1":
                    pages = pagecount.search(temp)
                    if not pages:
                        pages = 1
                    else:
                        pages = pages.group(1)
                        for j in xrange(2, int(pages) + 1):
                            r.rpush('postids', str(post) + ":" + str(j))
                #Open a file and dump the HTML for the post page to disk.
                POST = open("posts/%s-%s.html" % (str(post), str(page)), "w")
                print >> POST, temp
                POST.close()
    EXISTENCE.close()

if __name__ == "__main__":
    pagecount = re.compile("Page 1 of ([0-9]+)")
    post_re = re.compile("t=([0-9]+)")
    if len(sys.argv) < 2:
        print "Usage: posts-mt.py <# of cores> <master IP>"
        sys.exit(1)
    else:
        main()
