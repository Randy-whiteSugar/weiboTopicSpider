#根据关键词爬取新浪微博数据
from bs4 import BeautifulSoup
import requests
import re
import csv



def getHTMLText(url):
    cookie={"Cookie":"SINAGLOBAL=3715519605236.5015.1530444665069; _s_tentry=-; Apache=7068209816583.588.1555400822120; ULV=1555400822127:16:1:1:7068209816583.588.1555400822120:1554005971569; WBtopGlobal_register_version=edef3632d17f5fb3; crossidccode=CODE-gz-1HgiOv-15KOSg-dRnpFbQMRDWdl4T356333; ALF=1586936854; SSOLoginState=1555400855; SCF=AvNJ_yDl0UC-uPAIxzGbkWNnOOqrbnu5ylsmRGbfdMYskw3bDSzMTEGlLab4u8wrPdrXeQp0uYNfwqvOz8Kwnos.; SUB=_2A25xsfjIDeRhGeNI7loY-SbOzzSIHXVSx20ArDV8PUNbmtBeLWH9kW9NSFvzp3AwZFpcfpeyMYql9DN0QkHQ6rZl; SUBP=0033WrSXqPxfM725Ws9jqgMF55529P9D9W566ilk2OJ9yPBzLFWMyUHv5JpX5KzhUgL.Fo-cSKn41KnEShn2dJLoI0YLxKnLBo5LBKqLxKBLBo.LB.-LxKqLBo-L1h-LxK-L1h5LBK.LxKqLBozLBK2LxKqL1-eL1h.LxK.L1KzLBo2t; SUHB=0qj0SJJXG9BpUG; wvr=6; UOR=,,graph.qq.com; WBStorage=201904161547|undefined; webim_unReadCount=%7B%22time%22%3A1555400895311%2C%22dm_pub_total%22%3A3%2C%22chat_group_pc%22%3A0%2C%22allcountNum%22%3A3%2C%22msgbox%22%3A0%7D"}#换成你自己的cookie
    header = {
        'User-Agent': "'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64; rv:59.0) Gecko/20100101 Firefox/59.0'"}
    try:
        r=requests.get(url,timeout=30,cookies=cookie,headers=header)
        r.raise_for_status()
        r.encoding='utf-8'
        return r.text
    except:
        return""

def get_maxpage(url):
    html=getHTMLText(url)
    soup=BeautifulSoup(html,'html.parser')
    try:
       result = soup.find('div', class_='m-page').find_all('li')
       str = result[len(result) - 1].get_text()
       return int(re.findall("\d+", str)[0])
    except:
        return int('1')

def get_weiboInfo(slist,url):
    html = getHTMLText(url)
    soup = BeautifulSoup(html, 'html.parser')
    content=soup.find_all('div',attrs={'class':'content'})
    i = 0
    for card in content:
        i = i+1
        #获取微博博主链接和博主姓名
        user_url='https:'+card.find(attrs={'class':'name'}).attrs['href']
        user_name=card.find(attrs={'class':'name'}).attrs['nick-name']
        #获取微博内容（包括转发内容）
        all_content=card.find_all(attrs={'class':'txt'})
        if (len(all_content)<=1):
            weibo_content = all_content[0].get_text().strip()
        elif (len(all_content)>=2)&(all_content[1].attrs['node-type'].__eq__('feed_list_content_full')):
            weibo_content=all_content[1].get_text().strip()
        else:
            weibo_content=all_content[0].get_text().strip()+'//'+all_content[1].get_text().strip()
        try:
           #获取发布时间和微博博文链接
          if len(card.find_all(attrs={'class':'from'}))>1:
            created_at = card.find_all('p', class_='from')[len(card.find_all('p', class_='from'))-1].a.get_text().strip().replace("\n", "")
            content_url='https:'+card.find_all(attrs={'class':'from'})[len(card.find_all('p', class_='from'))-1].find('a').attrs['href']
          else:
            created_at = card.find('p', class_='from').find('a').get_text().strip().replace("\n", "")
            content_url='https:'+card.find(attrs={'class':'from'}).a.attrs['href']
        except:
            print("有条数据好像出错了")
            continue

        #获取收藏、转发、评论、点赞的数据
        all_count=soup.find_all('div',attrs={'class':'card-act'})
        collection_count=re.sub(r'\D',"",all_count[i-1].find_all('li')[0].get_text())
        reposts_count =re.sub(r'\D',"",all_count[i-1].find_all('li')[1].get_text())
        comments_count = re.sub(r'\D',"",all_count[i-1].find_all('li')[2].get_text())
        attitudes_count = re.sub(r'\D',"",all_count[i-1].find_all('li')[3].get_text())

       #获取用户id和微博id
        info = all_count[i-1].find_all('li')[1].a['action-data']
        list = re.search('mid=(\d+)&name=(.*?)&uid=(\d+).*?', info, re.S)
        mid = list.group(1)
        # print(mid)
        uid = list.group(3)

        if user_name.__eq__(''):
            print("爬虫出错了，请尽快检查cookie")

        temp=[]
        temp.append(uid)
        temp.append(user_name)
        temp.append(user_url)
        temp.append(str("'"+mid))
        temp.append(content_url)
        temp.append(weibo_content)
        temp.append(created_at)
        temp.append(collection_count)
        temp.append(reposts_count)
        temp.append(comments_count)
        temp.append(attitudes_count)
        slist.append(temp)

def write_data(data):
    with open(r"E0715.csv", 'a+', errors='ignore', newline='') as f:
            f_csv = csv.writer(f)
            f_csv.writerows(data)

def main():
    quote='长生生物'
    timeline=['2018-07-30-0:2018-07-30-1','2018-07-30-1:2018-07-30-2','2018-07-30-2:2018-07-30-3','2018-07-30-3:2018-07-30-4','2018-07-30-4:2018-07-30-5','2018-07-30-5:2018-07-30-6','2018-07-30-6:2018-07-30-7','2018-07-30-7:2018-07-30-8','2018-07-30-8:2018-07-30-9','2018-07-30-9:2018-07-30-10','2018-07-30-10:2018-07-30-11','2018-07-30-11:2018-07-30-12','2018-07-30-12:2018-07-30-13','2018-07-30-13:2018-07-30-14','2018-07-30-14:2018-07-30-15','2018-07-30-15:2018-07-30-16','2018-07-30-16:2018-07-30-17','2018-07-30-17:2018-07-30-18','2018-07-30-18:2018-07-30-19','2018-07-30-19:2018-07-30-20','2018-07-30-20:2018-07-30-21','2018-07-30-21:2018-07-30-22','2018-07-30-22:2018-07-30-23','2018-07-30-23:2018-07-31-0']


    # start_url='http://s.weibo.com/weibo/?q='+quote+'&typeall=1&suball=1&timescope=custom:'+timeline[0]+'&Refer=g&page=1'
    # max_page=get_maxpage(getHTMLText(start_url))
    # print(max_page)
    for i in timeline:
       slist=[]
       urla='http://s.weibo.com/weibo/?q='+quote+'&typeall=1&suball=1&timescope=custom:'+i+'&Refer=g&page=1'
       max_page = get_maxpage(urla)
       print(max_page)
       for page in range(max_page):
           url='http://s.weibo.com/weibo/?q='+quote+'&typeall=1&suball=1&timescope=custom:'+i+'&Refer=g&page='+str(page)
           get_weiboInfo(slist,url)
       write_data(slist)
       print('已经爬完'+i)
    print("you did it!!!")
main()