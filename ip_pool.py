# 列表方式
import redis
import time
import datetime
import json
import requests
from findw3.utils.email import send_mail
import json

# # 从接口获取ip
def get_proxies_from_api():
    try:
        ip_pools=[]
        # 每次返回200个代理ip
        API_url = f"http://api.3ip.cn/dmgetip.asp?apikey=490417f7&pwd=8bad3ab85e5021e9c1d293e79237c88e&getnum=200&httptype=0&geshi=2&fenge=1&fengefu=&Contenttype=1&operate=all"
        
        # header=get_header(COOKIE_IP)
        header={
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36'
        }
        r=requests.get(API_url,headers=header)

        Etext = r.text.encode('utf-8')
        # print(Etext)
        data=json.loads(Etext)

        if 'success' in data:
            if data['success'] == True:
                for ip in data['data']:
                    proxy_host=ip['ip']+':'+str(ip['port'])
                    proxies = {
                        'http': 'socks5://'+proxy_host,
                        'https': 'socks5://'+proxy_host,
                    }
                    proxies=json.dumps(proxies)
                    ip_pools.append(proxies)
        
    except Exception as e:
        print(f"{datetime.datetime.now()} - get_proxies_from_api - {e}")
    
    return ip_pools

def try_get_proxies_from_api():
    result = get_proxies_from_api()
    if len(result) > 0:
        return result
    else:
        res=[]
        try_count = 0
        while True:
            try_count += 1
            result = get_proxies_from_api()
            if len(result) > 0:
                res = result
                break
            time.sleep(1)
            if (try_count % 20 == 0):
                send_mail(f'无法从外部接口获取代理IP！', '1842523553@qq.com')
        # if len(res) == 0:
        #     send_mail(f'从外部接口取到的IP数量为0！', '1842523553@qq.com')
        # else:
        return res
        
# 连接redis
def connect(host = '192.168.123.101',port = 6379,db = 2):
    redis_host = host
    redis_port = port
    redis_db = db
    redis_conn = redis.Redis(host=redis_host, port=redis_port, db=redis_db)
    return redis_conn

def get_length():
    s=connect()
    list_name = 'proxies_redis_list'
    return s.llen(list_name)

# 将代理 IP 推入 Redis 列表
def push_proxy_to_redis(redis_conn,list_name,proxy):
    redis_conn.lpush(list_name, *proxy)

def pop_proxy_from_redis(redis_conn,list_name = 'proxies_redis_list'):
    # 从 Redis 列表中弹出一个代理 IP
    a = redis_conn.lpop(list_name)
    return a

def get_ip():
    s=connect()
    a=pop_proxy_from_redis(s)
    i=0
    while a is None:
        a=pop_proxy_from_redis(s)
        i+=1
        if i % 20 == 0:
            send_mail(f'IP池中多次获取无数据！','1842523553@qq.com')
            print(f'IP池中多次获取无数据！')
        time.sleep(20)
    # data_str = a.decode('utf-8')
    data_dict = json.loads(a)
    return data_dict

def get_all_elements(redis_conn, list_name):
    # 获取 Redis 列表中的所有元素
    elements = redis_conn.lrange(list_name, 0, -1)
    return elements

def clear_redis_list(redis_conn, list_name):
    # 使用 ltrim 将列表修剪为空列表
    redis_conn.ltrim(list_name, 1, 0)

# 每搁一段时间，清空代理IP池，重新获取代理IP放入数据库
def push_proxies_to_redis_list(redis_conn,list_name):
    try:
        while True:
            proxy=try_get_proxies_from_api()
            clear_redis_list(redis_conn,list_name)
            push_proxy_to_redis(redis_conn,list_name,proxy)
            time.sleep(20)
    except Exception as e:
        print(f'{datetime.datetime.now()} - push_proxies_to_redis_list - {e}')
    finally:
        clear_redis_list(redis_conn,list_name)


def requests_get_ip():
    s=connect()
    a=pop_proxy_from_redis(s)
    i=0
    # try:
    while i < 6:
        time.sleep(1)
        a=pop_proxy_from_redis(s)
        i+=1
        if a is not None:
            break
        if i == 5:
            raise ValueError(f'IP池中多次获取无数据！')
            # print(f'IP池中多次获取无数据！')
    data_dict = json.loads(a)
    # except Exception as e:

    return data_dict

def requests_ip_pool(url,headers=None,timeout=10,max_retries=10,type='n'):
    if type != 'n' and type != 'y':
        raise ValueError('传入的type参数类型不正确')

    if headers is None:
        headers={
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36'
        }

    if type == 'n':
        count = 0
        while count < max_retries:
            count += 1
            try:
                result = requests.get(url,headers=headers,timeout=timeout)
                return result
            except Exception as e:
                print(f'请求第{max_retries}次，失败！ - {e}')
            return None
    
    elif type == 'y':
        count = 0
        while count < max_retries:
            count += 1
            try:
                proxies = requests_get_ip()
                result = requests.get(url,headers=headers,timeout=timeout,proxies=proxies)
                return result
            except Exception as e:
                print(f'请求失败！ - {e}')
            return None


if __name__ == '__main__':
    try:
        s = connect()
        list_name = 'proxies_redis_list'
        clear_redis_list(s,list_name)
        # 启动IP池
        push_proxies_to_redis_list(s,list_name)

    except Exception as e:
        send_mail(f'IP池任务出错！- {datetime.datetime.now()} - 任务已结束')
        print(f'{datetime.datetime.now()} - main - {e}')
    except KeyboardInterrupt:
        clear_redis_list(s,list_name)
    finally:
        clear_redis_list(s,list_name)


    # print(get_length())


    # s = connect()
    # list_name = 'proxies_redis_list'
    # clear_redis_list(s,list_name)