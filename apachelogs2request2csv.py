# -*- coding: utf-8 -*-

import sys
import getopt
import requests
import time
import threading
import apachelogs2array
from AsyncCsvWriterByMap import AsyncCsvWriterByMap
from AsyncCsvWriterBySequentialMap import AsyncCsvWriterBySequentialMap

def request(url, counter, sequentialRequestCounter, ranAt, starttime):

    url = url_prepend + url

    started = time.time() - starttime

    exception = "N/A"
    status_code = -1
    requestResult = ""

    elapsed_start_real = time.perf_counter()
    try:
        response = requests.get(url)
        elapsed = response.elapsed.total_seconds()
        status_code = response.status_code
        result2csvSummary.incrementType(counter, "SUCCESS")
        requestResult = "SUCCESS"
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
            format(ranAt, '.5f').replace(".", ","),
            format(started, '.5f').replace(".", ","),
            format(started - ranAt, '.5f').replace(".", ","),
            format(elapsed * 1000, '.5f').replace(".", ","),
            format(elapsed_real * 1000, '.5f').replace(".", ","),
            url,
            str(status_code),
            requestResult,
            exception
        ])

def main(argv):

    opts, args = getopt.getopt(argv,"hi:s:d:p:")

    global url_prepend

    input_accesslog = ""
    output_summary = ""
    output_detailed = ""
    url_prepend = ""

    for opt, arg in opts:
        if opt == '-h':
            print('-i <apache access log input file> -s <output summary file> -d <output detailed file>')
            sys.exit()
        elif opt in ("-i"):
            input_accesslog = arg
        elif opt in ("-s"):
            output_summary = arg
        elif opt in ("-d"):
            output_detailed = arg
        elif opt in ("-p"):
            url_prepend = arg


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
        "QueuedThread",
        "StartedThread",
        "ThreadDiff",
        "Request_Elapsed",
        "Request_Elapsed_Real",
        "URL",
        "Status_Code",
        "RequestResult",
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
            for req in request_map[tick]:
                threading.Thread(target=request, args=[req[1], counter, sequentialRequestCounter, now - starttime, starttime]).start()
                sequentialRequestCounter = sequentialRequestCounter + 1

        counter = counter + 1
        if (counter - 1) >= last_key:
            break

        time.sleep(sleep)
    
    
if __name__ == "__main__":
    main(sys.argv[1:])