import traceback

import requests
from lxml import etree

def get_userID(info, cookie):
    url = info.xpath("div/a[@class='nk']/@href")[0]
    if r'/u/' in url:
        user_id = url[url.find('u/') + 2:]
    else:
        Referer = 'https://weibo.cn/search/?pos=search'
        Cookie = cookie
        User_Agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:66.0) Gecko/20100101 Firefox/66.0'
        headers = {
            'Cookie': Cookie,
            'User-Agent': User_Agent,
            'Referer': Referer
        }
        res = get_html(use_proxy=True, url=url, headers=headers, params={})
        html = etree.HTML(res.text.encode('utf-8'))
        id_href = html.xpath("//div[@class='u']//div[@class='ut']/a[text()='资料']/@href")[0]
        user_id = id_href[1:id_href.rfind('/')]

    print("用户ID：", user_id)
    return user_id

def get_proxy():
    return requests.get("http://127.0.0.1:5010/get/").json()

def delete_proxy(proxy):
    requests.get("http://127.0.0.1:5010/delete/?proxy={}".format(proxy))

#your spider code
def get_html(use_proxy, url, headers, params ,count = 1):
    # ....
    retry_count = 5
    proxy = get_proxy().get("proxy")
    while retry_count > 0:
        try:
            if use_proxy:
                html = requests.get(url, headers=headers, params=params, proxies={"http": "http://{}".format(proxy)})
            # 使用代理访问
            else:
                html = get_html_without_proxy(url, headers, params)
            return html
        except Exception:
            retry_count -= 1
    # 出错5次, 删除代理池中代理
    delete_proxy(proxy)
    if count < 2:
        return get_html(use_proxy, url, headers, params, count=count +1)
    else:
        return None

proxy = None
# proxy_url = 'http://127.0.0.1:5000/get'
# def get_proxy():
#     try:
#         r = requests.get(proxy_url)
#         r.encoding = r.apparent_encoding
#         if r.status_code == 200:
#             return r.text
#         else:
#             return None
#     except Exception as e:
#         print(e.args)
#         return None

def get_html_without_proxy(url, headers, params):
    try:
        r = requests.get(url=url, timeout=30, headers=headers, params=params,
                         allow_redirects=False)  # 不允许跳转页面,不使用代理
        r.raise_for_status()
        return r
    except Exception as e:
        print('Error: ', e)
        traceback.print_exc()

# def get_html(use_proxy, url, headers, params, count=1):
#     if use_proxy:
#         if count >= 5:
#             print('Tried too many times，proxy IP pool died')
#             return get_html_without_proxy(url, headers, params)
#         global proxy
#         try:
#             if not proxy:
#                 proxy = get_proxy()
#             if proxy:
#                 proxies = {
#                     'http': 'http://' + proxy
#                 }
#                 r = requests.get(url, timeout=30, headers=headers, params=params,
#                                  allow_redirects=False,
#                                  proxies=proxies)  # 不允许跳转页面,使用代理
#                 r.raise_for_status()
#                 return r
#             else:
#                 count += 1
#                 return get_html(use_proxy=True, url=url, count=count, params=params, headers=headers)
#         except:
#             count += 1
#             return get_html(use_proxy=True, url=url, count=count, params=params, headers=headers)
#     else:
#         return get_html_without_proxy(url, headers, params)

def get_cookie(Cookie, count=1):
    # from urllib import request
    # from http import cookiejar
    #
    # # 声明一个CookieJar对象实例来保存cookie
    # cookie = cookiejar.CookieJar()
    # # 利用urllib.request库的HTTPCookieProcessor对象来创建cookie处理器,也就CookieHandler
    # handler = request.HTTPCookieProcessor(cookie)
    # # 通过CookieHandler创建opener
    # opener = request.build_opener(handler)
    # # 此处的open方法打开网页
    # response = opener.open('https://www.weibo.cn')
    # # 打印cookie信息
    # cookie = ''
    # for name, value in spider():
    #     cookie = cookie + name + '=' + value
    retry_count = 5
    proxy = get_proxy().get("proxy")
    while retry_count > 0:
        try:
            User_Agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:66.0) Gecko/20100101 Firefox/66.0'

            headers = {
                'Cookie': Cookie,
                'User-Agent': User_Agent,
                'Referer': ''
            }
            res = requests.get('https://weibo.cn', headers=headers, proxies={"http": "http://{}".format(proxy)})
            return res.cookies
        except Exception:
            retry_count -= 1
    # 出错5次, 删除代理池中代理
    delete_proxy(proxy)
    return get_cookie(Cookie, count=count+1)
    # response = requests.get("https://weibo.cn")
    # cookie_value = ''
    # for key, value in response.cookies.items():
    #     cookie_value += key + '=' + value + ';'
    #
    # return cookie_value