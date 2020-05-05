import json
import re
import sys
import time
import traceback
from datetime import timedelta, datetime
from random import randint

import xlrd
from lxml import etree
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
import os
import excelSave as save
from weibo_user_mysql import Weibo

def deal_time(info):
    """获取微博发布时间"""
    try:
        str_time = str(info.xpath("string(div/span[@class='ct'])"))
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
        print('微博发布时间: ' + publish_time +'\n')
        return publish_time
    except Exception as e:
        print('Error: ', e)
        traceback.print_exc()

def deal_footer(info):
    try:
        footer = {}
        pattern = r'\d+'
        str_footer = str(info.xpath('string(div[last()])'))
        str_footer = str_footer[str_footer.rfind('赞'):]
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

def deal_content(info):
    try:
        content_unlinked = ''.join(info.xpath("div/span[@class ='ctt'][1]/text()"))
        content_linked = str(info.xpath("string(div/span[@class ='ctt'][1])"))
        print("微博内容", content_linked)
        return content_unlinked, content_linked
    except Exception as e:
        print('Error: ', e)
        traceback.print_exc()

# 用来控制页面滚动
def Transfer_Clicks(driver):
    time.sleep(5)
    try:
        driver.execute_script("window.scrollBy(0,document.body.scrollHeight)", "")
    except:
        pass
    return "Transfer successfully \n"

#插入数据
def parse_and_insert_data(elems, book_name_xls, sheet_name_xls):
    value = []
    workbook = xlrd.open_workbook(book_name_xls)  # 打开工作簿
    sheets = workbook.sheet_names()  # 获取工作簿中的所有表格
    worksheet = workbook.sheet_by_name(sheets[0])  # sheet_name_xls)  # 获取工作簿中所有表格中的的第一个表格
    rows_old = worksheet.nrows  # 获取表格中已存在的数据的行数
    rid = rows_old

    for info in elems:
        #微博ID
        weibo_id = info.xpath('@id')[0][2:]
        print("微博ID：", weibo_id)

        # 用户名
        username = info.xpath('div/a/text()')[0]  # elem.find_elements_by_css_selector('a.nk')[0].text
        print("用户名：", username)

        #用户ID
        url = info.xpath("div/a[@class='nk']/@href")[0]
        if r'/u/' in url:
            user_id = url[url.find('u/') + 2:]
        else:
            user_id = url[url.find('cn/') + 3:]
        print("用户ID：", user_id)

        #微博内容
        weibo_content = deal_content(info)
        unlinked_weibo_content = weibo_content[0]
        linked_weibo_content = weibo_content[1]

        #转评赞
        footer = deal_footer(info)
        likes = footer['up_num']  # 微博点赞数
        shares = footer['retweet_num']  # 转发数
        comments = footer['comment_num']  # 评论数

        #发布时间
        publish_time = deal_time(info)

        value1 = [rid, weibo_id, username, user_id, unlinked_weibo_content, linked_weibo_content, shares, comments, likes, publish_time]
        value.append(value1)
        rid += 1

    print("当前插入第%d条原创微博" % (rid -1 ))

    #插入当前微博数据并返回用户ID列表
    user_id_set = save.write_excel_xls_append_noRepeat(book_name_xls, value)
    get_user_info(user_id_set)

def get_user_info(user_id_set):
    try:
        config_path = os.path.split(
            os.path.realpath(__file__))[0] + os.sep + 'config.json'
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

#获取数据
def get_current_weibo_data(book_name_xls, sheet_name_xls):
    #开始爬取数据
    page = 1
    timeToSleep = 5
    while page < 101:
        Transfer_Clicks(driver)
        time.sleep(randint(1, 2))
        res = driver.find_element_by_xpath("//*").get_attribute("outerHTML")
        html = etree.HTML(res)
        weibos = html.xpath("//div[@class='c' and @id]")
        parse_and_insert_data(weibos, book_name_xls, sheet_name_xls)

        #定时休眠
        if page > timeToSleep:
            print("抓取到%d页数据，插入当前新抓取数据并休眠2-5秒" % page)
            timeToSleep = timeToSleep + 5
            time.sleep(randint(2, 5))

        if page < 100:
            while 1:
                try:
                    driver.find_elements_by_xpath('//div[@id="pagelist"]/form/div/a')[0].click()
                    break
                except:
                    time.sleep(2)
                    print('等待加载下一页')
        print('-----------------------------------------------------')
        page += 1

#爬虫运行
def spider(username,password,driver,book_name_xls,sheet_name_xls,keyword):
    #创建文件
    if os.path.exists(book_name_xls):
        print("文件已存在")
    else:
        print("文件不存在，重新创建")
        value_title = [["序号","微博ID", "用户名称","用户ID", "无链接微博内容", "有链接微博内容", "微博转发量","微博评论量","微博点赞","发布时间"],]
        save.write_excel_xls(book_name_xls, sheet_name_xls, value_title)
    
    #加载驱动，使用浏览器打开指定网址  
    driver.set_window_size(452, 790)
    driver.get("https://passport.weibo.cn/signin/login")
    print("开始自动登陆，若出现验证码手动验证")
    time.sleep(3)

    elem = driver.find_element_by_xpath("//*[@id='loginName']")
    elem.send_keys(username)
    elem = driver.find_element_by_xpath("//*[@id='loginPassword']")
    elem.send_keys(password)
    elem = driver.find_element_by_xpath("//*[@id='loginAction']")
    elem.send_keys(Keys.ENTER)
    print("暂停20秒，用于验证码验证")
    time.sleep(20)

    # 添加cookie
    # cookie = {"_T_WM":"49621012173","ALF":"1586766646","M_WEIBOCN_PARAMS":"luicode=10000011&lfid=100103type%3D61%26q%3D%E6%AD%A6%E6%B1%89%E8%82%BA%E7%82%8E%26t%3D0&uicode=20000174&fid=1076035973256807","MLOGIN":"1","SCF":"AqM1jTQa31BCGO95D_HkILBtQ-kORPCLv7vHh7dp4dZfKVO74iIXo7oFt4wkxEFCgNci5Y3waOLutPRBNxzmlik.","SSOLoginState":"1584174647","SUB":"_2A25zaOZnDeRhGeNH6FsR9C_OyjiIHXVQkoovrDV6PUNbktAKLXXQkW1NSqmEAXXmVEfXYUmy9HLL5suezEnebZLu","SUBP":"0033WrSXqPxfM725Ws9jqgMF55529P9D9W5CusJp2wW6Mz-cwh-Hk7rN5JpX5KMhUgL.Fo-4e0.7Sh2EeKB2dJLoIEBLxKqLBo5LBoBLxKqLBonL122LxK-L1h-L1hnLxKML1KBL1-qt","SUHB":"0HThEi4W6dvYPq","WEIBOCN_FROM":"1110006030","XSRF-TOKEN":"9dff34"}
    # for ix in cookie:
    #     driver.add_cookie(ix)
    # driver.get("https://m.weibo.cn")

    driver.get('https://weibo.cn/search/?tf=5_012')
    while True:
        try:
            driver.find_elements_by_css_selector('div.c.tip')
            break
        except:
            print('请等待转到weibo.cn/search')
            time.sleep(2)

    # 搜索关键词
    while True:
        try:
            elem = driver.find_element_by_xpath("/html/body/div[4]/form/div/input[1]")
            break
        except:
            time.sleep(2)
    elem.send_keys(keyword)
    elem.send_keys(Keys.ENTER)

    #点击原创按钮
    while True:
        try:
            elem = driver.find_element_by_xpath("/html/body/div[@class='tip']/a[1]")
            elem.click()
            print("已进入原创实时界面，休眠2秒")
            break
        except:
            print('请等待原创页面加载')
            time.sleep(2)

    #爬取实时
    get_current_weibo_data(book_name_xls, sheet_name_xls)
    time.sleep(2)

    
if __name__ == '__main__':
    username = "15927072248" #你的微博登录名
    password = "syc15232712894" #你的密码
    driver = webdriver.Chrome('C:\Program Files (x86)\Google\Chrome\Application\chromedriver')#你的chromedriver的地址

    #添加cookie
    # cookie = {"_T_WM":"49621012173","ALF":"1586766646","M_WEIBOCN_PARAMS":"luicode=10000011&lfid=100103type%3D61%26q%3D%E6%AD%A6%E6%B1%89%E8%82%BA%E7%82%8E%26t%3D0&uicode=20000174&fid=1076035973256807","MLOGIN":"1","SCF":"AqM1jTQa31BCGO95D_HkILBtQ-kORPCLv7vHh7dp4dZfKVO74iIXo7oFt4wkxEFCgNci5Y3waOLutPRBNxzmlik.","SSOLoginState":"1584174647","SUB":"_2A25zaOZnDeRhGeNH6FsR9C_OyjiIHXVQkoovrDV6PUNbktAKLXXQkW1NSqmEAXXmVEfXYUmy9HLL5suezEnebZLu","SUBP":"0033WrSXqPxfM725Ws9jqgMF55529P9D9W5CusJp2wW6Mz-cwh-Hk7rN5JpX5KMhUgL.Fo-4e0.7Sh2EeKB2dJLoIEBLxKqLBo5LBoBLxKqLBonL122LxK-L1h-L1hnLxKML1KBL1-qt","SUHB":"0HThEi4W6dvYPq","WEIBOCN_FROM":"1110006030","XSRF-TOKEN":"9dff34"}
    # for k, v in cookie.items():
    #     driver.add_cookie({'name': k, 'value': v, 'domain': 'https://m.weibo.cn'})

    keywords = ["武汉肺炎",] # 此处可以设置多个关键词

    for keyword in keywords:
        book_name_xls = keyword + ".xls"  # 填写你想存放excel的路径，没有文件会自动创建
        sheet_name_xls = keyword  # sheet表名
        spider(username, password, driver, book_name_xls, sheet_name_xls, keyword)
