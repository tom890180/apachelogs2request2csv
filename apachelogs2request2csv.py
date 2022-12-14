# -*- coding: utf-8 -*-

import sys
import getopt
import requests
import time
import threading
import apachelogs2array
from AsyncCsvWriterByMap import AsyncCsvWriterByMap
from AsyncCsvWriterBySequentialMap import AsyncCsvWriterBySequentialMap
from datetime import datetime
import urllib

ongoing_requests = 0

def cachestatus2numeric(status):
    if status == "HIT": return 1
    if status == "REVALIDATED": return 2
    if status == "UPDATING": return 3
    if status == "STALE": return 4
    if status == "EXPIRED": return 5
    if status == "BYPASS": return 6
    if status == "MISS": return 7
    return -1


def request(url, counter, sequentialRequestCounter, ranAt, starttime):

    url = url_prepend + url
    global ongoing_requests

    started = time.time() - starttime

    exception = "N/A"
    status_code = -1
    requestResult = ""
    xcacheresult = ""
    xcacheresultnumeric = "-1"

    elapsed_start_real = time.perf_counter()
    try:
        response = urllib.request.urlopen(url)
        elapsed = 0
        status_code = response.status
        result2csvSummary.incrementType(counter, "SUCCESS")
        requestResult = "SUCCESS"
    
        xcacheresult = str(response.getheader('X-Cache-Status'))
        xcacheresultnumeric = str(cachestatus2numeric(xcacheresult))

    except Exception as e:
        print("CONNECTION ERROR")
        elapsed = 0
        exception = "{}".format(e)

        result2csvSummary.incrementType(counter, "FAIL")
        requestResult = "FAIL"
    finally:
        elapsed_real = time.perf_counter() - elapsed_start_real
        result2csvDetailed.setValue(sequentialRequestCounter, [
            str(sequentialRequestCounter),
            str(counter),
            str(ongoing_requests),
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            format(ranAt, '.5f').replace(".", ","),
            format(started, '.5f').replace(".", ","),
            format(started - ranAt, '.5f').replace(".", ","),
            format(elapsed * 1000, '.5f').replace(".", ","),
            format(elapsed_real * 1000, '.5f').replace(".", ","),
            url,
            str(status_code),
            requestResult,
            xcacheresult,
            xcacheresultnumeric,
            exception
        ])
        ongoing_requests = ongoing_requests - 1

def main(argv):

    opts, args = getopt.getopt(argv,"hi:s:d:p:r:")

    global url_prepend
    global ongoing_requests

    input_accesslog = ""
    output_summary = ""
    output_detailed = ""
    url_prepend = ""
    max_requests_per_second = 0

    for opt, arg in opts:
        if opt == '-h':
            print('-i <apache access log input file> -s <output summary file> -d <output detailed file> -r <max_requests_per_second, none: 0>')
            sys.exit()
        elif opt in ("-i"):
            input_accesslog = arg
        elif opt in ("-s"):
            output_summary = arg
        elif opt in ("-d"):
            output_detailed = arg
        elif opt in ("-p"):
            url_prepend = arg
        elif opt in ("-r"):
            max_requests_per_second = int(arg)


    request_map, request_count_map, last_key, total_requests = apachelogs2array.parse(input_accesslog)

    global result2csvDetailed
    global result2csvSummary

    result2csvSummary = AsyncCsvWriterByMap(request_count_map, [
        'SUCCESS',
        'FAIL'
    ], output_summary)
    result2csvDetailed = AsyncCsvWriterBySequentialMap(total_requests, [
        "Index",
        "Second",
        "Ongoing_Requests",
        "real_time",
        "QueuedThread",
        "StartedThread",
        "ThreadDiff",
        "Request_Elapsed",
        "Request_Elapsed_Real",
        "URL",
        "Status_Code",
        "RequestResult",
        "X-Cache-Status",
        "X-Cache-Status2",
        "Exception"
    ], output_detailed)

    counter = 0
    sequentialRequestCounter = 0
    starttime = time.time()
    print("STARTED: {}, will run for {} seconds".format(starttime, last_key))

    while True:
        now = time.time()
        sleep = 1 - ((now - starttime) % 1)
        tick = int(now - starttime)
        print("tick {}, sleep {}, sec {}".format(now - starttime, sleep, tick))

        if tick in request_map:
            requestsOnSecond = 0
            for req in request_map[tick]:
                if max_requests_per_second != 0 and max_requests_per_second < requestsOnSecond: break

                threading.Thread(target=request, args=[req[1], counter, sequentialRequestCounter, now - starttime, starttime]).start()
                sequentialRequestCounter = sequentialRequestCounter + 1
                requestsOnSecond = requestsOnSecond + 1
                ongoing_requests = ongoing_requests + 1

        counter = counter + 1
        if (counter - 1) >= last_key:
            break

        time.sleep(sleep)
    
    
if __name__ == "__main__":
    main(sys.argv[1:])
