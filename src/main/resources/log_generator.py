import random
import time


url_path = [
    "class/112.html",
    "class/128.html",
    "class/145.html",
    "class/146.html",
    "class/131.html",
    "class/130.html",
    "class/145.html",
    "learn/821",
    "course/list",
]

ip_slices = [132, 156, 124, 10, 29, 167, 143, 187, 30, 46, 55, 63, 72, 87, 98, 168]


http_refer = [
    "http://www.baidu.com/s?wd={query}",
    "https://www.sougou.com/web?query={query}",
    "http://cn.bing.com/search?q={query}",
    "https://www.so.com/s?p={query}"
]

search_keyword = [
    "Spark SQL 实战",
    "Hadoop基础",
    "Storm实战",
    "Spark Streaming实战",
    "大数据面试"
]

status_code = ["200", "404", "500", "499"]

# url部分
def sample_url():
    return random.sample(url_path, 1)[0]

#ip 部分
def sample_ip():
    # 取四个ip部分
    slice = random.sample(ip_slices, 4)
    return ".".join(str(_) for _ in slice)

# refer部分
def sample_refer():
    if (random.uniform(0, 1) < 0.2):
        return "_"
    refer_str = random.sample(http_refer, 1)
    query_str = random.sample(search_keyword, 1)
    return refer_str[0].format(query = query_str[0])

# 状态码
def sample_status_code():
    return random.sample(status_code, 1)[0]

# 日期


def generate_log(count = 10):
    time_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

    with open("/home/xh/bigdata/data/projects/logs/access.log", "w+") as f:
        while(count >= 1):
            # ""中 \" 才能输出"
            query_log = "{ip}\t{local_time}\t\"GET /{url} HTTP/1.1\"\t{status_code}\t{refer}".format(local_time = time_str, url = sample_url(), ip=sample_ip(), refer=sample_refer(), status_code=sample_status_code())
            count -= 1
            f.write(query_log + "\n")
            print(query_log)
    f.close()



if __name__ == '__main__':
    generate_log(5)