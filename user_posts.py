import requests
import re
import json
from findw_spiders.utils.funcs import *
import pandas as pd
import time
import findw3.api.clickhouse as ch
from findw3.utils.email import send_mail
from config import MAX_TRY_COUNT,TIME_OUT,COOKIE_IP
from findw3.utils.ip_pool import get_ip
from queue import Queue
import datetime
from findw_spiders.utils.funcs import csv_append
import threading
import os

COOKIES = [COOKIE_IP]

# 储存cookie的队列
cookie_queue = Queue()

# 储存用户id的队列
user_id_queue = Queue()

user_id_dict = {}

t_date_file_name= pd.to_datetime(datetime.datetime.today()).strftime('%Y-%m-%d')

update_data_file_path='/data/tmp/data_capital/update_day_snowball/posts_info/new'
# update_data_file_path = '2024-05-15'

engine_info={'db_name': 'ark', 'user': 'ark', 'pwd': 'E2718281828', 'ip': '192.168.123.101'}


def add_user_id_to_queue():

    # 找出每个user_id以及最新的帖子创建时间
    def get_max_date():
        sql = f'''
                select a.user_id,if(max_updated is NULL,0,0) as max_updated 
                from
                (
                    select user_id from ark.d_user_info_snowball
                    where fan_num >= 50000
                    and status < 2
                ) as a
                left JOIN 
                (
                    select user_id,MAX(created_time) as max_updated
                    from ark.d_user_posts_snowball
                    group by user_id
                )as b
                on a.user_id = b.user_id
            '''
        df = ch.read(sql,engine_info=engine_info)
        global user_id_dict
        user_id_dict = df.set_index('user_id')['max_updated'].to_dict()

    get_max_date()
    
    user_id_list = list(user_id_dict.keys())
    for user_id in user_id_list:
        user_id_queue.put(user_id)


# 添加cookie到队列
def add_cookie_to_queue():
    try:
        for cookie in COOKIES:
            cookie_queue.put(cookie)
    except Exception as e:
        print(f"add_cookie_to_queue - {e}")


# 队列取cookie
def get_cookie():
    while cookie_queue.empty():
        time.sleep(1)
    return cookie_queue.get()

# 还队列cookie
def return_cookie(header):
    cookie=header['cookie']
    cookie_queue.put(cookie)
    
    
# 获取请求头
def get_header():
    cookie=get_cookie()
    header = {
        'authority':'stock.xueqiu.com',
        'method':'GET',
        'path':'/v5/stock/portfolio/stock/list.json?size=1000&category=3&uid=9742512811&pid=-24',
        'scheme':'https',
        'accept':'application/json, text/plain, */*',
        'accept-encoding':'gzip, deflate, br',
        'accept-language':'zh-CN,zh;q=0.9',
        'cookie':cookie,
        'origin':'https://xueqiu.com',
        # 'referer':'https://xueqiu.com/u/9742512811',
        'sec-ch-ua':'";Not A Brand";v="99", "Chromium";v="94"',
        'sec-ch-ua-mobile':'?0',
        'sec-ch-ua-platform':'"Windows"',
        'sec-fetch-dest':'empty',
        'sec-fetch-mode':'cors',
        'sec-fetch-site':'same-site',
        'user-agent':'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.71 Safari/537.36 Core/1.94.208.400 QQBrowser/12.0.5440.400'
    }
    return header


def search_for_data(user_id,page):

    def get_requ(user_id, page):
        time.sleep(5)
        is_return = 0
        '''
        :param    user_id   用户ID
        :param    page      页数
        :param    sort      新帖(time) / 热帖(alpha)
        '''
        url = f'https://xueqiu.com/v4/statuses/user_timeline.json?page={page}&user_id={user_id}&type=0'

        try:
            header = get_header()
            is_return=1
            proxies=get_ip()
            # data = request_data(url, proxy=False, headers=header, proxy_seq=2)
            ret = requests.get(url, headers=header,proxies=proxies,timeout=TIME_OUT)
            return_cookie(header)
            is_return=2

            data = json.loads(ret.text)

            if 'error_description' in data and data['error_description'] == '用户不存在':
                return {}
            else:
                return data
        except Exception as e:
            if(is_return == 1):
                return_cookie(header)
                is_return=2
            return None
    
    result=get_requ(user_id, page)
    if result is not None:
        return result
    else:
        result = []
        try_count=0
        while try_count<MAX_TRY_COUNT:
            res=get_requ(user_id, page)
            try_count+=1
            if res is not None:
                result=res
                break
        return result


def analy_user_posts(posts_data,user_id):
    # if (len(posts_data) == 0) or (posts_data['total'] == 0):
    #     return pd.DataFrame(columns=[
    #                 'user_id','post_id','created_time','content',
    #                 'view_num','retweet_num','reply_num',
    #                 'like_num','reward_num','source'])

    # 检查是否是更新数据
    def check_res(df,user_id):
        max_old_updated = int(user_id_dict[user_id])
        df_new = df[df.created_time >  max_old_updated]
        if len(df_new) > 0:
            return df
        else:
            return None

    data_ls = []
    for item in posts_data['statuses']:
        # 评论ID
        post_id = item['id']
        # 评论内容
        content = item['description']
        content = re.sub(r'<[^>]+>', "",  content)
        content = content.replace(',','!..')
        content = content.replace('，','!..')

        # 发布时间
        # created_at = pd.to_datetime(item['created_at']*1e6+(8*3600*1e9))
        created_at = item['created_at']
        # 浏览次数
        view_count = item['view_count']
        # 转发数
        retweet_count = item['retweet_count']
        # 评论数
        reply_count = item['reply_count']
        # 赞数
        like_count = item['like_count']
        # 收藏数
        reward_count = item['reward_count']
        # 设备
        source = item['source']
        
        data_ls.append({
            'user_id': user_id,
            'post_id': post_id,
            'created_time': created_at,
            'content': content,
            'view_num': view_count,
            'retweet_num': retweet_count,
            'reply_num': reply_count,
            'like_num': like_count,
            'reward_num': reward_count,
            'source': source,
        })
    
    if len(data_ls) > 0:
        df = pd.DataFrame(data_ls)
        df = check_res(df,user_id)

        return df
    else:
        return None


def get_data():
    while not user_id_queue.empty():
        print(f'{datetime.datetime.now()} - {user_id_queue.qsize()}')
        try:
            user_id = user_id_queue.get()

            df_ls = []

            page = 1
            posts_data = search_for_data(user_id, page)
            max_page = 999
            if (len(posts_data) == 0) or (posts_data['total'] == 0):
                max_page = 0
            else:
                max_page = posts_data['maxPage']
                res = analy_user_posts(posts_data,user_id)
                if res is None:
                    max_page = 0
                else:
                    df_ls.append(res)

            while page < max_page:
                page += 1
                posts_data = search_for_data(user_id, page)
                if (len(posts_data) == 0) or (posts_data['total'] == 0):
                    max_page = 0
                else:
                    res = analy_user_posts(posts_data,user_id)
                    if res is None:
                        max_page = 0
                    else:
                        df_ls.append(res)

            if len(df_ls) > 0:
                df = pd.concat(df_ls)
                csv_append(f'/data/tmp/data_capital/update_day_snowball/posts_info/new/{t_date_file_name}/{user_id}.csv',df)
        except Exception as e:
            print('************************')
            print(f"get_date - {e}")
            print(df_ls)
            print('************************')


def data_to_sql():
    try:
        sql_old = f'''
            select distinct post_id from ark.d_user_posts_snowball
        '''
        df_old = ch.read(sql_old,engine_info=engine_info)

        path_dir = f"{update_data_file_path}/{t_date_file_name}"
        file_ls = os.listdir(path_dir)
        if file_ls:
            df_ls = []
            for file in file_ls:
                try:
                    path_file = os.path.join(path_dir, file)
                    df_dl_a = pd.read_csv(path_file, encoding='utf-8',dtype=str)
                    df_ls.append(df_dl_a)
                except Exception as e:
                    send_mail(f'获取用户发帖数据解析错误{file} - {e}','1842523553@qq.com')
                    print(f'获取用户发帖数据解析错误{file} - {e}')
            df = pd.concat(df_ls,ignore_index=True)
        df = df.drop_duplicates(subset='post_id')

        def is_integer(x):
            try:
                int(x)  # 尝试转换为整数
                return True
            except ValueError:
                send_mail(f'用户发帖数据解析数据出错 - {x}','1842523553@qq.com')
                print(f'用户发帖数据解析数据出错 - {x}')
                return False

        # 使用apply()函数过滤掉不能转换为整数的行
        df = df[df['user_id'].apply(is_integer)]
        df = df[df['post_id'].apply(is_integer)]

        df['post_id'] = df['post_id'].astype('int')
        df['user_id'] = df['user_id'].astype('int')

        df = df[~df.post_id.isin(df_old.post_id)]
        df['t_date'] = t_date_file_name

        ch.insert(df,'d_user_posts_snowball',engine_info=engine_info)
        send_mail(f'用户发帖数据更新完成，{t_date_file_name}','1842523553@qq.com')
    except Exception as e:
        send_mail(f'用户发帖数据存入数据库出错 - {e}，{t_date_file_name}','1842523553@qq.com')


def main():

    s_time = pd.to_datetime(datetime.datetime.now())

    print(s_time)

    print(f'发帖信息更新开始:{t_date_file_name}')

    # 创建临时数据存储文件夹
    if not os.path.exists(f"{update_data_file_path}/{t_date_file_name}"):
        os.makedirs(f"{update_data_file_path}/{t_date_file_name}")

    # 创建多个线程
    num_threads = 1
    threads = []

    add_user_id_to_queue()
    add_cookie_to_queue()

    for _ in range(num_threads):
        thread = threading.Thread(target=get_data)
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()

    e_time=pd.to_datetime(datetime.datetime.now())

    print(f"{s_time} - {e_time}")
    print(f"用时：{e_time-s_time}")

    data_to_sql()
    
    e_time = pd.to_datetime(datetime.datetime.now())

    print(f'{s_time} - {e_time}')
    print(f'用时：{e_time-s_time}')

    print('组合数据获取重试结束')

if __name__ == '__main__':
    main()
    # data_to_sql()

        

