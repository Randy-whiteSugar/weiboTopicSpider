# -*- coding: utf-8 -*-
# author:           inspurer(月小水长)
# pc_type           lenovo
# create_time:      2020/1/12 22:04
# file_name:        main.py
# github            https://github.com/inspurer
# qq邮箱            2391527690@qq.com
# 微信公众号         月小水长(ID: inspurer)


import requests

from lxml import etree
from collections import OrderedDict

import csv

import traceback

import random

import re

from time import sleep

import os

from datetime import datetime, timedelta
import sys
from threading import Thread

Cookie='SINAGLOBAL=9655056417769.03.1581500505941; _s_tentry=login.sina.com.cn; UOR=xxgqt.youth.cn,widget.weibo.com,login.sina.com.cn; Apache=7852788502806.352.1583906665777; ULV=1583906665788:4:1:1:7852788502806.352.1583906665777:1582274208925; WBStorage=42212210b087ca50|undefined; WBtopGlobal_register_version=3d5b6de7399dfbdb; SCF=AmYj8uPfxFJ7f_p56PU6wo21kcOPSCb3zt0WCulJqPWQoVvVYCz7Sh_eenApHADkJo9XB8w2J8ox89d20TZbxbY.; SUB=_2A25zbA_TDeRhGeNH6FsR9C_OyjiIHXVQGGYbrDV8PUNbmtAfLRmjkW9NSqmEAS_E3fZLL_Oz3j0mzSY9sObTnG4x; SUBP=0033WrSXqPxfM725Ws9jqgMF55529P9D9W5CusJp2wW6Mz-cwh-Hk7rN5JpX5K2hUgL.Fo-4e0.7Sh2EeKB2dJLoIEBLxKqLBo5LBoBLxKqLBonL122LxK-L1h-L1hnLxKML1KBL1-qt; SUHB=0N21h-ab93GxqX; ALF=1584511489; SSOLoginState=1583906691; un=15927072248',

headers = {
        'Cookie': Cookie,
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36'
        }
proxy_url = 'http://127.0.0.1:5000/get'  # 要注意这里！不能写localhost:5000/get
proxy = None
max_count = 5

class WeiboTopicScrapy(Thread):

    def __init__(self,keyword,limit_date):
        Thread.__init__(self)
        self.keyword = keyword
        self.limit_date = limit_date
        self.flag = True
        self.got_num = 0  # 爬取到的微博数
        self.weibo = []  # 存储爬取到的所有微博信息
        if not os.path.exists('topic'):
            os.mkdir('topic')
        self.start()

    def get_html2(self, url, headers, params):
        try:
            r = requests.get(url, timeout=30, headers=headers, params=params)
            r.raise_for_status()
            r.encoding = r.apparent_encoding
            if r.status_code == 200:
                return r.content
        except Exception as e:
            print('Error: ', e)
            traceback.print_exc()

    def get_proxy(self):
        try:
            r = requests.get(proxy_url)
            r.encoding = r.apparent_encoding
            if r.status_code == 200:
                return r.text
            else:
                return None
        except Exception as e:
            print(e.args)
            return None

    #TODO text or content!
    def get_html(self, url, headers, params, count=1, use_agent=False):
        #print('Clawling', url)
        if use_agent:
            #print('Trying Count', count)
            if count >= max_count:
                print('Tried too many times')
                return None
            global proxy
            try:
                if not proxy:
                    proxy = self.get_proxy()
                if proxy:
                    proxies = {
                        'http': 'http://' + proxy
                    }
                    r = requests.get(url, timeout=30, headers=headers, params=params,
                                     allow_redirects=False,
                                     proxies=proxies)  # 不允许跳转页面,使用代理
                    r.encoding = r.apparent_encoding
                    if r.status_code == 200:
                        return r.text
                    else:
                        return self.get_html(url=url, count=count, params=params, headers=headers)  # 公共代理容易出现多人同时使用而被封，因此不要设置最大更换次数
                else:
                    print('Get Proxy Failed')
                    return self.get_html(url=url, count=count, params=params, headers=headers)
            except ConnectionError as e:
                print('Error Occured', e.args)
                proxy = self.get_proxy()
                count += 1
                return self.get_html(url=url, count=count, params=params, headers=headers)
        else:
            r = requests.get(url=url, timeout=30, headers=headers, params=params, allow_redirects=False)  # 不允许跳转页面,不使用代理
            r.encoding = r.apparent_encoding
            if r.status_code == 200:
                return r.text
            else:
                return self.get_html(url=url, headers=headers, params=params)

    def deal_html(self,url):
        """处理html"""
        try:
            #.content返回二进制数据，.text返回unicode数据
            html = requests.get(url, headers=self.headers).content
            selector = etree.HTML(html)
            return selector
        except Exception as e:
            print('Error: ', e)
            traceback.print_exc()

    def deal_garbled(self, str_info):
        """处理乱码"""
        try:
            #http://c.biancheng.net/view/4305.html
            info = str_info.replace(u'\u200b', u'').encode(sys.stdout.encoding, 'ignore').decode(sys.stdout.encoding)
            return info
        except Exception as e:
            print('Error: ', e)
            traceback.print_exc()

    def get_weibo_content(self,info):
        """获取原创微博内容"""
        try:
            # TODO 链接处理尝试
            a_text = info.xpath('div//a/text()')
            if u'展开全文' in a_text:
                full_content = ''.join(info.xpath('div//p[@node-type="feed_list_content_full"]/text()'))
            else:
                full_content = ''.join(info.xpath('div//p[@node-type="feed_list_content"]/text()'))
            weibo_content = self.deal_garbled(full_content.strip())
            print(weibo_content)
            return weibo_content.replace(',', '，')
        except Exception as e:
            print('Error: ', e)
            traceback.print_exc()

    def get_publish_time(self,info):
        """获取微博发布时间"""
        try:
            str_time = info.xpath("string(//p[@class='from'])").strip()
            publish_time = str_time.split('来自')[0].strip()
            if '秒' in publish_time:
                second = publish_time[:publish_time.find('秒')]
                second = timedelta(seconds =int(second))
                publish_time = (datetime.now() -
                                second).strftime('%Y-%m-%d %H:%M')
            elif '分钟' in publish_time:
                minute = publish_time[:publish_time.find('分钟')]
                minute = timedelta(minutes=int(minute))
                publish_time = (datetime.now() -
                                minute).strftime('%Y-%m-%d %H:%M')
            elif '今天' in publish_time:
                today = datetime.now().strftime('%Y-%m-%d')
                time = publish_time[3:]
                publish_time = today + ' ' + time
            elif '月' in publish_time:
                year = datetime.now().strftime('%Y')
                month = publish_time[0:2]
                day = publish_time[3:5]
                time = publish_time[7:12]
                publish_time = year + '-' + month + '-' + day + ' ' + time
            else:
                publish_time = publish_time[:16]
            print('微博发布时间: ' + publish_time)
            return publish_time
        except Exception as e:
            print('Error: ', e)
            traceback.print_exc()

    def get_weibo_footer(self, info):
        """获取微博点赞数、转发数、评论数"""
        try:
            footer = {}
            pattern = re.compile(r'\d+')
            footer_list = info.xpath("div//div[@class='card-act']/ul/li")

            retweet_num_str = footer_list[1].xpath('string(.)')
            retweet_num_str = pattern.search(retweet_num_str)
            if retweet_num_str:
                retweet_num = int(retweet_num_str.group(0))
            else:
                retweet_num = 0
            print('转发数: ' + str(retweet_num))
            footer['retweet_num'] = retweet_num

            comment_str = footer_list[2].xpath('string(.)')
            comment_str = pattern.search(comment_str)
            if comment_str:
                comment_num = int(comment_str.group(0))
            else:
                comment_num = 0
            print('评论数: ' + str(comment_num))
            footer['comment_num'] = comment_num

            up_str = footer_list[3].xpath('string(.)')
            up_str = pattern.search(up_str)
            if up_str:
                up_str = int(up_str.group(0))
            else:
                up_str = 0
            print('点赞数: ' + str(up_str))
            footer['up_num'] = up_str

            return footer
        except Exception as e:
            print('Error: ', e)
            traceback.print_exc()

    """def get_user_info(self, url):
        # TODO 代理使用
        user_id = url[url.find('com/') + 4: url.rfind('?')]
        real_url = 'https://weibo.cn/%s/info' % (user_id)
        r = requests.get(url=real_url, timeout=30, headers=headers, allow_redirects=True)
        html = etree.HTML(r.content)
        user_info = {}
        user_info['id'] = user_id
        try:
            user_num = html.xpath("//div[@id='Pl_Core_T8CustomTriColumn__3']//td[@class='S_line1'")
            follow_num = user_num[0].xpath('strong[@class="W_f18"]/text')
        except Exception as e:
            print('获取用户信息出错')
            print('Error: ', e)
            traceback.print_exc()
"""

    def get_one_weibo(self,info):
        """获取一条微博的全部信息"""
        try:
            weibo = OrderedDict()
            weibo['id'] = info.xpath('@mid')[0] #微博id
            print(weibo['id'])
            weibo['publisher'] = info.xpath("//div[@class='info']/div[2]/a[1]/text()")[0] #用户名
            print(weibo['publisher'])
            url = info.xpath("//div[@class='avator']/a/@href")[0]
            #user_info = self.get_user_info(url)

            weibo['publisher_id'] = url[url.find('com/') + 4: url.rfind('?')]
            print(weibo['publisher_id'])
            weibo['content'] = self.get_weibo_content(info)  # 微博内容
            weibo['publish_time'] = self.get_publish_time(info)  # 微博发布时间
            if weibo['publish_time'][:10] < self.limit_date:
                self.flag = False
            footer = self.get_weibo_footer(info)
            weibo['up_num'] = footer['up_num']  # 微博点赞数
            weibo['retweet_num'] = footer['retweet_num']  # 转发数
            weibo['comment_num'] = footer['comment_num']  # 评论数
            return weibo
        except Exception as e:
            print(str(info.xpath('string(.)')))
            print('Error: ', e)
            traceback.print_exc()

    def write_csv(self, wrote_num):
        """将爬取的信息写入csv文件"""
        try:
            result_headers = [
                '微博id',
                '用户名',
                '用户id',
                # '用户粉丝数',
                # '用户关注数',
                # '用户发博数',
                # '用户行业类别',
                # '用户认证信息',
                '微博正文',
                '发布时间',
                '点赞数',
                '转发数',
                '评论数',
            ]

            result_data = [w.values() for w in self.weibo][wrote_num:]

            with open('topic/'+self.keyword+'.csv', 'a', encoding='utf-8-sig', newline='') as f:
                writer = csv.writer(f)
                if wrote_num == 0:
                    writer.writerows([result_headers])
                writer.writerows(result_data)
            print('%d条微博写入csv文件完毕:' % self.got_num)
        except Exception as e:
            print('Error: ', e)
            traceback.print_exc()

    def run(self):
        wrote_num = 0
        page1 = 0
        random_pages = random.randint(1, 5)

        for page in range(1, 51):
            if not self.flag:
                break
            print('\n第{}页....\n'.format(page))

            params={
                "keyword": "武汉肺炎",
                "hideSearchFrame": "",
                "page": page
            }
            global headers
            res = self.get_html2(url='https://weibo.cn/search/mblog', params=params, headers=headers)
            html = etree.HTML(res)

            #获得所有微博数量和位置
            try:
                weibos = html.xpath("//div[@id='pl_feedlist_index']//div[@class='card-wrap']")

                for i in range(0, len(weibos)):
                    #解析每个微博
                    certain_weibo = weibos[i]
                    comment_tag = None
                    #TODO 不知道这个原创判定是否有效，有待验证
                    try:
                        comment_tag = certain_weibo.xpath("//div[@class='card-feed']/div[@class='content']/div[@class='card-comment'")
                    except:
                        pass
                    is_original = False if comment_tag else True
                    if is_original:
                        aweibo = self.get_one_weibo(info=certain_weibo)
                        if aweibo:
                            self.weibo.append(aweibo)
                            self.got_num += 1
                            print('-' * 100)
                #写入文件
                if page % 3 == 0 and self.got_num > wrote_num:  # 每爬3页写入一次文件
                    self.write_csv(wrote_num)
                    wrote_num = self.got_num

                # 通过加入随机等待避免被限制。爬虫速度过快容易被系统限制(一段时间后限
                # 制会自动解除)，加入随机等待模拟人的操作，可降低被系统限制的风险。默
                # 认是每爬取1到5页随机等待6到10秒，如果仍然被限，可适当增加sleep时间
                if page - page1 == random_pages:
                    sleep(random.randint(2, 5))
                    page1 = page
                    random_pages = random.randint(1, 5)
            except:
                print("以下网页发生解析错误", res.text)

        if self.got_num > wrote_num:
            self.write_csv(wrote_num)  # 将剩余不足3页的微博写入文件

        print('共爬取' + str(self.got_num) + '条原创微博')

if __name__ == '__main__':
    # keyword_list = ["武汉肺炎", "新冠肺炎", "疫情"]
    # for keyword in keyword_list:
    #     WeiboTopicScrapy(keyword=keyword, limit_date='2019-12-30')
    WeiboTopicScrapy(keyword='武汉肺炎', limit_date='2019-12-30')










