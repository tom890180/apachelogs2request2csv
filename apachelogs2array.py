
from apachelogs import LogParser

def parse(path):

    loghandler = open(path, "r")
    request_map = {}
    request_count_map = {}
    total_requests = 0
    
    first_second = -1
    first_minute = -1

    parser = LogParser("%h %l %u %t \"%r\" %>s %b \"%{Referer}i\" \"%{User-Agent}i\"")

    for line in loghandler:

        entry = parser.parse(line)

        if first_second == -1:
            first_second = int(entry.request_time.strftime('%S'))
            first_minute = int(entry.request_time.strftime('%M'))


            key = 0
        else:
            second = abs(int(entry.request_time.strftime('%S')))
            minute = abs(int(entry.request_time.strftime('%M')))

            if first_minute == minute:
                key = (((minute - first_minute) * 60) + second) - first_second
            else:
                key = (((minute - first_minute) * 60) + second) - first_second

        if not key in request_map:
            request_map[key] = []
            request_count_map[key] = 0

        request_map[key].append(entry.request_line.split(" "))
        request_count_map[key] = request_count_map[key] + 1

        total_requests = total_requests + 1
        

    return request_map, request_count_map, list(request_map)[-1], total_requests