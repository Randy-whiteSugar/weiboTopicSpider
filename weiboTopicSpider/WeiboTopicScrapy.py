# -*- coding: utf-8 -*-
# author:           inspurer(月小水长)
# pc_type           lenovo
# create_time:      2020/1/12 22:04
# file_name:        main.py
# github            https://github.com/inspurer
# qq邮箱            2391527690@qq.com
# 微信公众号         月小水长(ID: inspurer)
import json

from linkage_userID import *

import requests

from lxml import etree
from collections import OrderedDict

from urllib.parse import quote

import csv

import traceback

import random

import re

from time import sleep

import os

from datetime import datetime, timedelta
import sys

#from cnselenium import spider
from weibo_user_mysql import Weibo

User_Agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:66.0) Gecko/20100101 Firefox/66.0'

#Cookie = get_cookie(Cookie)

def get_user_info(user_id_set):
    try:
        config_path = os.path.split(
            os.path.realpath('__file__'))[0] + os.sep + 'config.json'
        if not os.path.isfile(config_path):
            sys.exit(u'当前路径：%s 不存在配置文件config.json' %
                     (os.path.split(os.path.realpath(__file__))[0] + os.sep))
        with open(config_path) as f:
            try:
                config = json.loads(f.read())
            except ValueError:
                sys.exit(u'config.json 格式不正确，请参考 '
                         u'https://github.com/dataabc/weibo-crawler#3程序设置')

        config['user_id_list'].extend(list(user_id_set))
        wb = Weibo(config)
        wb.start()  # 爬取微博信息
    except Exception as e:
        print('Error: ', e)
        traceback.print_exc()

class WeiboTopicScrapy():#Thread):

    def __init__(self,keyword,filter,start_time,end_time):
        #Thread.__init__(self)
        self.headers={
            'Cookie': Cookie,
            'User_Agent': User_Agent
        }
        self.keyword = keyword
        self.filter = filter # 1: 原创微博； 0：所有微博
        self.start_time = start_time
        self.end_time = end_time
        self.got_num = 0  # 爬取到的微博数
        self.real_wrote = 0 #本次爬取实际写入的微博数
        self.weibo = []  # 存储爬取到的所有微博信息
        if not os.path.exists('topic'):
            os.mkdir('topic')
        #self.start()

    def deal_html(self,url):
        """处理html"""
        try:
            html = requests.get(url, headers=self.headers).content
            selector = etree.HTML(html)
            return selector
        except Exception as e:
            print('Error: ', e)
            traceback.print_exc()

    def deal_garbled(self,info):
        """处理乱码"""
        try:
            info = (info.xpath('string(.)').replace(u'\u200b', '').encode(
                sys.stdout.encoding, 'ignore').decode(sys.stdout.encoding))
            return info
        except Exception as e:
            print('Error: ', e)
            traceback.print_exc()

    def get_long_weibo(self,weibo_link):
        """获取长原创微博"""
        try:
            selector = self.deal_html(weibo_link)
            info = selector.xpath("//div[@class='c']")[1]
            wb_content = self.deal_garbled(info)
            wb_time = info.xpath("//span[@class='ct']/text()")[0]
            weibo_content = wb_content[wb_content.find(':') +
                                       1:wb_content.rfind(wb_time)]
            return weibo_content
        except Exception as e:
            print('Error: ', e)
            traceback.print_exc()

    def get_original_weibo(self,info, weibo_id):
        """获取原创微博"""
        try:
            weibo_content = self.deal_garbled(info)
            weibo_content = weibo_content[:weibo_content.rfind('赞[')]
            a_text = info.xpath('div//a/text()')
            if u'全文' in a_text:
                weibo_link = 'https://weibo.cn/comment/' + weibo_id
                wb_content = self.get_long_weibo(weibo_link)
                if wb_content:
                    weibo_content = wb_content
            return weibo_content
        except Exception as e:
            print('Error: ', e)
            traceback.print_exc()

    def get_long_retweet(self,weibo_link):
        """获取长转发微博"""
        try:
            wb_content = self.get_long_weibo(weibo_link)
            weibo_content = wb_content[:wb_content.rfind('原文转发')]
            return weibo_content
        except Exception as e:
            print('Error: ', e)
            traceback.print_exc()

    def get_retweet(self,info, weibo_id):
        """获取转发微博"""
        try:
            original_user = info.xpath("div/span[@class='cmt']/a/text()")
            if not original_user:
                wb_content = '转发微博已被删除'
                return wb_content
            else:
                original_user = original_user[0]
            wb_content = self.deal_garbled(info)
            wb_content = wb_content[wb_content.find(':') +
                                    1:wb_content.rfind('赞')]
            wb_content = wb_content[:wb_content.rfind('赞')]
            a_text = info.xpath('div//a/text()')
            if '全文' in a_text:
                weibo_link = 'https://weibo.cn/comment/' + weibo_id
                weibo_content = self.get_long_retweet(weibo_link)
                if weibo_content:
                    wb_content = weibo_content
            retweet_reason = self.deal_garbled(info.xpath('div')[-1])
            retweet_reason = retweet_reason[:retweet_reason.rindex('赞')]
            wb_content = (retweet_reason + '\n' + '原始用户: ' + original_user +
                          '\n' + '转发内容: ' + wb_content)
            return wb_content
        except Exception as e:
            print('Error: ', e)
            traceback.print_exc()

    def get_weibo_content(self,info, is_original):
        """获取微博内容"""
        # try:
        #     weibo_id = info.xpath('@id')[0][2:]
        #     if is_original:
        #         weibo_content = self.get_original_weibo(info, weibo_id)
        #     else:
        #         weibo_content = self.get_retweet(info, weibo_id)
        #     print(weibo_content)
        #     return weibo_content
        # except Exception as e:
        #     print('Error: ', e)
        #     traceback.print_exc()
        try:
            content_unlinked = ''.join(info.xpath("div/span[@class ='ctt'][1]/text()"))[1:]
            #content_linked = str(info.xpath("string(div/span[@class ='ctt'][1])"))
            print("微博内容:", content_unlinked)   #content_linked)
            return content_unlinked   #, content_linked
        except Exception as e:
            print('Error: ', e)
            traceback.print_exc()

    def get_publish_place(self,info):
        """获取微博发布位置"""
        try:
            div_first = info.xpath('div')[0]
            a_list = div_first.xpath('a')
            publish_place = '无'
            for a in a_list:
                if ('place.weibo.com' in a.xpath('@href')[0]
                        and a.xpath('text()')[0] == '显示地图'):
                    weibo_a = div_first.xpath("span[@class='ctt']/a")
                    if len(weibo_a) >= 1:
                        publish_place = weibo_a[-1]
                        if ('视频' == div_first.xpath(
                                "span[@class='ctt']/a/text()")[-1][-2:]):
                            if len(weibo_a) >= 2:
                                publish_place = weibo_a[-2]
                            else:
                                publish_place = '无'
                        publish_place = self.deal_garbled(publish_place)
                        break
            print('微博发布位置: ' + publish_place)
            return publish_place
        except Exception as e:
            print('Error: ', e)
            traceback.print_exc()

    def get_publish_time(self,info):
        """获取微博发布时间"""
        try:
            str_time = info.xpath("div/span[@class='ct']")
            str_time = self.deal_garbled(str_time[0])
            publish_time = str_time.split('来自')[0]
            if '刚刚' in publish_time:
                publish_time = datetime.now().strftime('%Y-%m-%d %H:%M')
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

    def get_publish_tool(self,info):
        """获取微博发布工具"""
        try:
            str_time = info.xpath("div/span[@class='ct']")
            str_time = self.deal_garbled(str_time[0])
            if len(str_time.split('来自')) > 1:
                publish_tool = str_time.split(u'来自')[1]
            else:
                publish_tool = '无'
            print('微博发布工具: ' + publish_tool)
            return publish_tool
        except Exception as e:
            print('Error: ', e)
            traceback.print_exc()

    def get_weibo_footer(self,info):
        """获取微博点赞数、转发数、评论数"""
        try:
            footer = {}
            pattern = r'\d+'
            str_footer = info.xpath('div')[-1]
            str_footer = self.deal_garbled(str_footer)
            str_footer = str_footer[str_footer.rfind('赞['):]
            weibo_footer = re.findall(pattern, str_footer, re.M)

            up_num = int(weibo_footer[0])
            print('点赞数: ' + str(up_num))
            footer['up_num'] = up_num

            retweet_num = int(weibo_footer[1])
            print('转发数: ' + str(retweet_num))
            footer['retweet_num'] = retweet_num

            comment_num = int(weibo_footer[2])
            print('评论数: ' + str(comment_num))
            footer['comment_num'] = comment_num
            return footer
        except Exception as e:
            print('Error: ', e)
            traceback.print_exc()

    def extract_picture_urls(self,info, weibo_id):
        """提取微博原始图片url"""
        try:
            a_list = info.xpath('div/a/@href')
            first_pic = 'https://weibo.cn/mblog/pic/' + weibo_id + '?rl=0'
            all_pic = 'https://weibo.cn/mblog/picAll/' + weibo_id + '?rl=1'
            if first_pic in a_list:
                if all_pic in a_list:
                    selector = self.deal_html(all_pic)
                    preview_picture_list = selector.xpath('//img/@src')
                    picture_list = [
                        p.replace('/thumb180/', '/large/')
                        for p in preview_picture_list
                    ]
                    picture_urls = ','.join(picture_list)
                else:
                    if info.xpath('.//img/@src'):
                        preview_picture = info.xpath('.//img/@src')[-1]
                        picture_urls = preview_picture.replace(
                            '/wap180/', '/large/')
                    else:
                        sys.exit(
                            "爬虫微博可能被设置成了'不显示图片'，请前往"
                            "'https://weibo.cn/account/customize/pic'，修改为'显示'"
                        )
            else:
                picture_urls = '无'
            return picture_urls
        except Exception as e:
            print('Error: ', e)
            traceback.print_exc()

    def get_picture_urls(self,info, is_original):
        """获取微博原始图片url"""
        try:
            weibo_id = info.xpath('@id')[0][2:]
            picture_urls = {}
            if is_original:
                original_pictures = self.extract_picture_urls(info, weibo_id)
                picture_urls['original_pictures'] = original_pictures
                if not self.filter:
                    picture_urls['retweet_pictures'] = '无'
            else:
                retweet_url = info.xpath("div/a[@class='cc']/@href")[0]
                retweet_id = retweet_url.split('/')[-1].split('?')[0]
                retweet_pictures = self.extract_picture_urls(info, retweet_id)
                picture_urls['retweet_pictures'] = retweet_pictures
                a_list = info.xpath('div[last()]/a/@href')
                original_picture = '无'
                for a in a_list:
                    if a.endswith(('.gif', '.jpeg', '.jpg', '.png')):
                        original_picture = a
                        break
                picture_urls['original_pictures'] = original_picture
            return picture_urls
        except Exception as e:
            print('Error: ', e)
            traceback.print_exc()

    def get_one_weibo(self,info):
        """获取一条微博的全部信息"""
        try:
            weibo = OrderedDict()
            is_original = False if len(info.xpath("div/span[@class='cmt']")) > 3 else True
            if (not self.filter) or is_original:
                weibo['id'] = info.xpath('@id')[0][2:]
                weibo['publisher'] = info.xpath('div/a/text()')[0]
                weibo['publisher_id'] = get_userID(info, Cookie)
                weibo['content'] = self.get_weibo_content(info,
                                                     is_original)  # 微博内容
                picture_urls = self.get_picture_urls(info, is_original)
                weibo['original_pictures'] = picture_urls[
                    'original_pictures']  # 原创图片url
                if not self.filter:
                    weibo['retweet_pictures'] = picture_urls[
                        'retweet_pictures']  # 转发图片url
                    weibo['original'] = is_original  # 是否原创微博
                weibo['publish_place'] = self.get_publish_place(info)  # 微博发布位置
                weibo['publish_time'] = self.get_publish_time(info)  # 微博发布时间
                weibo['publish_tool'] = self.get_publish_tool(info)  # 微博发布工具
                footer = self.get_weibo_footer(info)
                weibo['up_num'] = footer['up_num']  # 微博点赞数
                weibo['retweet_num'] = footer['retweet_num']  # 转发数
                weibo['comment_num'] = footer['comment_num']  # 评论数
            else:
                weibo = None
            return weibo
        except Exception as e:
            print('Error: ', e)
            traceback.print_exc()

    def write_csv(self,date):
        """将爬取的信息写入csv文件，加入了无重复ID的核验"""
        try:
            result_headers = [
                '微博id',
                '发布者',
                '发布者id',
                '微博正文',
                '原始图片url',
                '发布位置',
                '发布时间',
                '发布工具',
                '点赞数',
                '转发数',
                '评论数',
            ]
            if not self.filter:
                result_headers.insert(4, '被转发微博原始图片url')
                result_headers.insert(5, '是否为原创微博')

            filename = 'topic/' + self.keyword +'_hot_'+ date + '.csv'
            if not os.path.exists(filename):
                open(filename, 'w')

            with open(filename, 'r', encoding='utf-8-sig', newline='') as f:
                csv_reader = csv.reader(f)
                written_weibo_id_list = [value[0] for value in csv_reader][1:]
                result_data = [w.values() for w in self.weibo if w['id'] not in written_weibo_id_list]
                # 注意区分written_num和wrote_num
                # 前者是文件中已写入的数据行数，后者是本次爬取已写入的数据行数
                written_num = len(written_weibo_id_list)

            with open('topic/'+self.keyword +'_hot_'+ date +'.csv', 'a', encoding='utf-8-sig', newline='') as f:
                writer = csv.writer(f)
                if written_num == 0:
                    writer.writerows([result_headers])
                if result_data:
                    writer.writerows(result_data)
                    self.real_wrote += len(result_data)
                    print('本次爬取的前%d条微博写入csv文件完毕:' % self.real_wrote)
                else:
                    print("此次抓取的三页数据都已存在于csv文件中，均拒绝写入")
        except Exception as e:
            print('Error: ', e)
            traceback.print_exc()

    # def once_crawl(self, endtime):
    #     print("当前从"+endtime.strftime('%Y%m%d')+"日开始爬取")
    #     wrote_num = 0
    #     page1 = 0
    #     random_pages = random.randint(1, 3)
    #     pageNum = 10000000
    #
    #     user_id_set = set()
    #
    #     count = 0
    #
    #     #用于计数爬到空网页的个数，达到10个认为已受限
    #     for page in range(1, pageNum):
    #         print('\n\n第{}页....\n'.format(page))
    #         Referer = 'https://weibo.cn/search/mblog?hideSearchFrame=&keyword={}&page={}'.format(quote(self.keyword),
    #                                                                                              page - 1)
    #         headers = {
    #             'Cookie': Cookie,
    #             'User-Agent': User_Agent,
    #             'Referer': Referer
    #         }
    #         #此处改变时间参数
    #         params = {
    #             'hideSearchFrame': '',
    #             'keyword': self.keyword,
    #             'advancedfilter': '1',
    #             'starttime': self.start_time,
    #             'endtime': endtime.strftime('%Y%m%d'),
    #             'sort': 'time',
    #             'page': page
    #         }
    #         res = get_html(use_proxy=True, url='https://weibo.cn/search/mblog', params=params, headers=headers)
    #
    #         html = etree.HTML(res.text.encode('utf-8'))
    #
    #         try:
    #             weibos = html.xpath("//div[@class='c' and @id]")
    #
    #             # 判定是否爬到空网页
    #             if weibos:
    #                 for i in range(0, len(weibos)):
    #                     aweibo = self.get_one_weibo(info=weibos[i])
    #                     if aweibo:
    #                         # 此处获得用户id
    #                         user_id_set.add(aweibo['publisher_id'])
    #                         self.weibo.append(aweibo)
    #                         self.got_num += 1
    #                         print('-' * 100)
    #                     if len(user_id_set) >= 10:
    #                         get_user_info(user_id_set)
    #                         user_id_set = set()
    #                 # 每爬3页写入一次文件
    #                 if page % 3 == 0 and self.got_num > wrote_num:
    #                     self.write_csv(date)
    #                     wrote_num = self.got_num
    #             else:
    #                 # 空网页过多，改变时间段重新爬
    #                 count += 1
    #                 if count > 9:
    #                     endtime = datetime.strptime(self.weibo[-1]['publish_time'], '%Y-%m-%d %H:%M')
    #                     break
    #             # 通过加入随机等待避免被限制。爬虫速度过快容易被系统限制(一段时间后限
    #             # 制会自动解除)，加入随机等待模拟人的操作，可降低被系统限制的风险。默
    #             # 认是每爬取1到5页随机等待6到10秒，如果仍然被限，可适当增加sleep时间
    #             if page - page1 == random_pages and page < pageNum:
    #                 sleep(random.randint(6, 10))
    #                 page1 = page
    #                 random_pages = random.randint(1, 5)
    #         except Exception as e:
    #             print('Error: ', e)
    #             traceback.print_exc()
    #
    #         if self.got_num > wrote_num:
    #             self.write_csv()  # 将剩余不足3页的微博写入文件
    #         if not self.filter:
    #             print('共爬取' + str(self.got_num) + '条微博')
    #         else:
    #             print('共爬取' + str(self.got_num) + '条原创微博')
    #
    #     return endtime

    def run(self, Cookie):
        wrote_num = 0
        page1 = 0
        random_pages = random.randint(1, 3)
        pageNum = 10000000

        user_id_set = set()
        count = 0

        # endtime = datetime.strptime(self.end_time, '%Y%m%d')
        # starttime = datetime.strptime(self.start_time, '%Y%m%d')
        # while endtime - starttime > timedelta(days=1):
        #     endtime = self.once_crawl(endtime)
        for page in range(1, pageNum):
            print('\n\n第{}页....\n'.format(page))
            Referer = 'https://weibo.cn/search/mblog?hideSearchFrame=&keyword={}&page={}'.format(quote(self.keyword),
                                                                                                 page - 1)
            headers = {
                'Cookie': Cookie,
                'User-Agent': User_Agent,
                'Referer': Referer
            }
            params = {
                'hideSearchFrame': '',
                'keyword': self.keyword,
                'advancedfilter': '1',
                'starttime': self.start_time,
                'endtime': self.end_time,
                'sort': 'hot',
                'filter': 'hasori',
                'page': page
            }
            html = None
            retry_count = 3

            while retry_count > 0:
                try:
                    res = get_html(use_proxy=True, url='https://weibo.cn/search/mblog', params=params, headers=headers)
                    html = etree.HTML(res.text.encode('utf-8'))
                    break
                except Exception as e:
                    print('Error: ', e)
                    retry_count -= 1
            try:
                weibos = html.xpath("//div[@class='c' and @id]")
                if weibos:
                    count = 0
                    for i in range(0, len(weibos)):
                        aweibo = self.get_one_weibo(info=weibos[i])
                        if aweibo:
                            #此处获得用户id
                            user_id_set.add(aweibo['publisher_id'])
                            self.weibo.append(aweibo)
                            self.got_num += 1
                            print('-' * 100)
                        if len(user_id_set) >= 10:
                            get_user_info(user_id_set)
                            user_id_set = set()
                    # 每爬3页写入一次文件
                    if page % 3 == 0 and self.got_num > wrote_num:
                        self.write_csv(self.start_time)
                        wrote_num = self.got_num
                else:
                    # 空网页过多
                    count += 1
                    if count > 9:
                        break
                # 通过加入随机等待避免被限制。爬虫速度过快容易被系统限制(一段时间后限
                # 制会自动解除)，加入随机等待模拟人的操作，可降低被系统限制的风险。默
                # 认是每爬取1到5页随机等待6到10秒，如果仍然被限，可适当增加sleep时间
                if page - page1 == random_pages and page < pageNum:
                    sleep(random.randint(6, 10))
                    page1 = page
                    random_pages = random.randint(1, 3)
            except Exception as e:
                print('Error: ', e)
                traceback.print_exc()

        if self.got_num > wrote_num:
            self.write_csv(self.start_time)  # 将剩余不足3页的微博写入文件
        if not self.filter:
            print('共爬取' + str(self.got_num) + '条微博')
        else:
            print('共爬取' + str(self.got_num) + '条原创微博')
        #return Cookie


if __name__ == '__main__':
    #filter = 0 爬取所有微博，filter = 1 爬取原创微博
    Cookie = '''ALF=1586779567; SCF=AmYj8uPfxFJ7f_p56PU6wo21kcOPSCb3zt0WCulJqPWQ_ndNLwxrjNBEr5igv1F5jN8GcY_KjBNln6bP9olQcFk.; SUB=_2A25zaLjgDeRhGeBL4lQV9CbEyDqIHXVQktiorDV6PUJbktANLWetkW1NRqgZKTFtfz0u6Yh-S7ZhyaqRl-ckJKo9; SUBP=0033WrSXqPxfM725Ws9jqgMF55529P9D9WhfgbcxNB7bOiEB7.aAyBVB5JpX5K-hUgL.Foqf1KqXShnRe0q2dJLoI0zLxK-L1hnL1hMLxKMLBoeL1K-LxKnL1KnLBonLxKML1K.LB.BLxKMLBoeL1Kq0eKeXSntt; SUHB=0N2lR_9Jd3GzdE; _T_WM=55d1431cb00f8dd895558431e210f163'''
    for i in range(16, 32):
        if i < 10:
            date = '2020010' + str(i)
        else:
            date = '202001' + str(i)
        WeiboTopicScrapy(keyword='肺炎', filter=1, start_time=date, end_time=date).run(Cookie)
    for i in range(1, 30):
        if i < 10:
            date = '2020020' + str(i)
        else:
            date = '202002' + str(i)
        WeiboTopicScrapy(keyword='肺炎', filter=1, start_time=date, end_time=date).run(Cookie)