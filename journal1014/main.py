"""
-------------------------------------------------
   File Name：     main.py
   Description :   爬虫主程序
   Author :        Cai Yufan
   date：          2019/10/14
-------------------------------------------------
   Change Activity:
    add IP proxy module
    change to abuyun

    每次到下一页都会改一次IP：IP更改太乱了
    或者可以每隔10分钟改一次IP：固定更改，不灵活
    给IP加锁？行不通，代理服务器自动变换IP，无法加锁

    大问题：
    多线程IP仍有可能出错，因为每一篇文章，需要三次请求，两次预请求一次下载，这之间有时间差，
    若IP在这个时间差内失效，IP更改将出错。
    小问题：
    check_ip_valid 太过频繁，可能导致无法请求而出错
-------------------------------------------------
"""
import requests
import re
import time, os, shutil, logging
from UserInput import get_uesr_inpt
from GetConfig import config
# from CrackVerifyCode import crack
from GetPageDetail import PageDetail
# 引入字节编码
from urllib.parse import quote
# 引入beautifulsoup
from bs4 import BeautifulSoup
import threading
import queue
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import kdl
import random
HEADER = config.crawl_headers
# 获取cookie
BASIC_URL = 'http://kns.cnki.net/kns/brief/result.aspx'
# 利用post请求先行注册一次
SEARCH_HANDLE_URL = 'http://kns.cnki.net/kns/request/SearchHandler.ashx'
# 发送get请求获得文献资源
GET_PAGE_URL = 'http://kns.cnki.net/kns/brief/brief.aspx?pagename='
# 下载的基础链接
DOWNLOAD_URL = 'http://kns.cnki.net/kns/'
# 切换页面基础链接
CHANGE_PAGE_URL = 'http://kns.cnki.net/kns/brief/brief.aspx'

#################################################################
# 代理服务器
proxyHost = "http-cla.abuyun.com"
proxyPort = "9030"

# 代理隧道验证信息
proxyUser = "HN9CNRBIIRFC671C"
proxyPass = "5799B743F9F08540"

proxyMeta = "http://%(user)s:%(pass)s@%(host)s:%(port)s" % {
  "host" : proxyHost,
  "port" : proxyPort,
  "user" : proxyUser,
  "pass" : proxyPass,
}

proxies = {
    "http": proxyMeta,
    "https": proxyMeta,
}


def get_one_ip():
    print("try to get one new ip.")
    if not check_ip_valid(proxies):
        # print("get one new ip success.")
        update_ip(proxies)
    else:
        # print("old ip still work.")
        pass
    return proxies


def check_ip_valid(ip_json):
    targetUrl = "http://test.abuyun.com"
    resp = requests.get(targetUrl, proxies=ip_json)
    # print("check_ip_valid:", resp.status_code == 200)
    return resp.status_code == 200


# 用于detail page的ip更新
def update_ip(proxy):
    targetUrl = "http://proxy.abuyun.com/switch-ip"
    resp = requests.get(targetUrl, proxies=proxy)
    print("新的IP：", resp.text)
    return

##################################################################


class SearchTools(object):
    '''
    构建搜索类
    实现搜索方法
    '''

    def __init__(self, cnt):
        session = requests.Session()
        self.session = session
        self.cur_page_num = 1
        # 保持会话
        ip_json = get_one_ip()
        # print(ip)
        # print(ip_json)
        # self.ip_string = ip_json['http'][7:]
        self.ip_json = ip_json

        # self.ip_for_detail_string = ip_json['http'][7:]
        self.ip_for_detail_json = ip_json
        self.session.get(BASIC_URL, headers=HEADER, timeout=11, proxies=self.ip_json)

    def search_reference(self, ueser_input, item):
        '''
        第一次发送post请求
        再一次发送get请求,这次请求没有写文献等东西
        两次请求来获得文献列表
        '''
        static_post_data = {
            'action': '',
            'NaviCode': '*',
            'ua': '1.21',
            'isinEn': '1',
            'PageName': 'ASP.brief_default_result_aspx',
            'DbPrefix': 'SCDB',
            'DbCatalog': '中国学术期刊网络出版总库',
            'ConfigFile': 'CJFQ.xml',
            'db_opt': 'CJFQ',  # 搜索类别（CNKI右侧的）
            'db_value': '中国学术期刊网络出版总库',
            'his': '0',
            'db_cjfqview': '中国学术期刊网络出版总库,WWJD',
            'db_cflqview': '中国学术期刊网络出版总库',
            '__': time.asctime(time.localtime()) + ' GMT+0800 (中国标准时间)'
        }
        # 将固定字段与自定义字段组合
        post_data = {**static_post_data, **ueser_input}
        # print(post_data)
        # 必须有第一次请求，否则会提示服务器没有用户
        first_post_res = self.session.post(
            SEARCH_HANDLE_URL, data=post_data, headers=HEADER, proxies=self.ip_json)
        # get请求中需要传入第一个检索条件的值
        key_value = quote(ueser_input.get('txt_1_value1'))
        self.get_result_url = GET_PAGE_URL + first_post_res.text + '&t=1544249384932&keyValue=' + key_value + '&S=1&sorttype='
        # 检索结果的第一个页面

        second_get_res = self.session.get(self.get_result_url,
                                          headers=HEADER, timeout=12, proxies=self.ip_json)
        change_page_pattern_compile = re.compile(
            r'.*?pagerTitleCell.*?<a href="(.*?)".*')
        try:
            self.change_page_url = re.search(change_page_pattern_compile,
                                             second_get_res.text).group(1)
        except:
            # print("该学校在该年无记录或名称错误")
            # global global_remain_page
            # global_remain_page = -1
            item.change_global_remain_page(-1)
            return 0
        return self.parse_page(
            self.pre_parse_page(second_get_res.text, item), second_get_res.text, item)

    def pre_parse_page(self, page_source, item):
        '''
        预爬取，检测有多少页，剩余多少页，用户选择需要检索的页数
        '''
        reference_num_pattern_compile = re.compile(r'.*?找到&nbsp;(.*?)&nbsp;')
        reference_num = re.search(reference_num_pattern_compile,
                                  page_source).group(1)
        reference_num_int = int(reference_num.replace(',', ''))
        # print('检索到' + reference_num + '条结果,全部下载大约需要' + s2h(reference_num_int * 5) + '。')
        # if word:
        #     is_all_download = 'y'
        # else:
        #     is_all_download = input('是否要全部下载（y/n）?')
        is_all_download = 'y'
        # 将所有数量根据每页20计算多少页
        if is_all_download == 'y':
            total_page, i = divmod(reference_num_int, 20)
            # print('总共', total_page+1, '页')
            if item.return_remain_page == 9999:
                select_download_page = item.return_start_page()
                self.cur_page_num = select_download_page
                # global global_first
                # if select_download_page == 1:
                #     global_first = True
                # else:
                #     global_first = False
                if select_download_page == 1:
                    item.change_global_first(1)
                else:
                    item.change_global_first(0)
            # -1 is use for this journal is not exist, see in the function search_reference() the except part.
            elif item.return_remain_page == -1:
                self.cur_page_num = 1
            else:
                # global global_current_page
                ######################################################################
                # self.cur_page_num = global_current_page
                self.cur_page_num = item.return_current_page()
                # download_page_left = item.return_remain_page()
                # return download_page_left

            download_page_left = total_page - self.cur_page_num
        if i != 0:
            download_page_left += 1
        # print("pre_parse_page download_page_left", download_page_left)
        return download_page_left

    def parse_page(self, download_page_left, page_source, item):
        '''
        保存页面信息
        解析每一页的下载地址
        '''
        soup = BeautifulSoup(page_source, 'lxml')
        # 定位到内容表区域
        tr_table = soup.find(name='table', attrs={'class': 'GridTableContent'})
        # 处理验证码, 如果出现验证码这步将失败，会进入exception，即重新刷新页面
        try:
            # 去除第一个tr标签（表头）
            # print("parse_page: ", download_page_left)
            tr_table.tr.extract()
        except Exception as e:
            # logging.error(e)
            ################################
            # 10.14 一旦出现验证码，直接换IP
            print("出现验证码")
            update_ip(proxies)
            ###############################

            # print("剩余的页数",download_page_left)
            # print("已经爬取的页数",self.cur_page_num)
            # global global_current_page
            # global_current_page = self.cur_page_num
            item.change_global_current_page(self.cur_page_num)
            return
        # 遍历每一行
        # print(len(tr_table.find_all(name='tr'))) #测试页面返回结果
        # print(len(list(enumerate(tr_table.find_all(name='tr')))))
        # global global_first #不再使用全局变量，而将其封装在Globalitem之中
        global_first = item.return_global_first()
        if not global_first:
            # global_first = True
            item.change_global_first(1)
            self.get_another_page(download_page_left, item)
            # print("global_first return")
            return
        # print(download_page_left, '\n', tr_table.find_all(name='tr'))
        # print(tr_table.find_all(name='tr'))
        if tr_table.find_all(name='tr') == []:
            # print("出现问题")
            # print("剩余的页数",download_page_left)
            # print("已经爬取的页数",self.cur_page_num)
            # global_current_page = self.cur_page_num
            item.change_global_current_page(self.cur_page_num)
            return
        # print(tr_table)
        for index, tr_info in enumerate(tr_table.find_all(name='tr')):
            # print(tr_info)
            single_refence_list = []
            try:
                tr_text = ''
                download_url = ''
                detail_url = ''
                author_url = ''
                # 遍历每一列
                for index, td_info in enumerate(tr_info.find_all(name='td')):
                    # 因为一列中的信息非常杂乱，此处进行二次拼接
                    # print(tr_info.find_all(name='td'))
                    td_text = ''
                    for string in td_info.stripped_strings:
                        td_text += string
                    tr_text += td_text + ' '
                    # 注意保存在各自以期刊名字为文件夹名的文件夹中
                    with open(item.return_name() + '/ReferenceList.txt', 'a', encoding='utf-8') as file:
                        file.write(td_text +' ')
                    # 寻找下载链接
                    dl_url = td_info.find('a', attrs={'class': 'briefDl_D'})
                    # 寻找详情链接
                    dt_url = td_info.find('a', attrs={'class': 'fz14'})
                    # 排除不是所需要的列
                    if dt_url:
                        detail_url = dt_url.attrs['href']
                    if dl_url:
                        download_url = dl_url.attrs['href']
                # 将每一篇文献的信息分组
                single_refence_list = tr_text.split(' ')
                # print(single_refence_list)
                self.download_refence(download_url, single_refence_list, item)
                # 是否开启详情页数据抓取
                # 主要有用的信息均在详情页抓取！！！！！！！！！！！！
                if not check_ip_valid(self.ip_for_detail_json):
                    print("下载详细页面时，IP过期")
                    update_ip(proxies)

                if config.crawl_isdetail == '1':
                    time.sleep(config.crawl_stepWaitTime)
                    item.get_page_detail.get_detail_page(self.session, self.get_result_url, detail_url,
                                                         single_refence_list, self.download_url,
                                                         self.cur_page_num, item.return_name(), self.ip_for_detail_json)
                # 下载作者的id到reference list
                try:
                    with open(item.return_name() + '/ReferenceList.txt', 'a', encoding='utf-8') as file:
                        for author_tmp in tr_info.find_all(class_='author_flag'):
                            author_url = author_tmp.a['href']
                            file.write(author_url + ' ')
                        # 在每一行结束后输入一个空行
                        file.write('\n')
                except:
                    # print("no author id")
                    pass

            # download_page_left为剩余等待遍历页面
            except Exception as e:
                ### 修改
                # print("get this line fail, log to fail_file: fail_pdf.txt")
                logging.error(e)
                fail_file = open("fail_pdf.txt", "a", encoding='utf-8')
                fail_file.write(single_refence_list[1]+'\n')
                # print(single_refence_list)
                fail_file.close()
                global FAIL
                FAIL = FAIL + 1
                # print('错误数目：', FAIL)
                # print("download_page_left:", download_page_left)

        download_page_left = download_page_left - 1
        # global global_remain_page
        # global_remain_page = download_page_left
        item.change_global_remain_page(download_page_left)
        if download_page_left >= 0:
            self.cur_page_num += 1
            item.change_global_current_page(self.cur_page_num)
            self.get_another_page(download_page_left, item)
        # print("parse page finials:", download_page_left)
        return

    def get_another_page(self, download_page_left, item):
        '''
        请求其他页面和请求第一个页面形式不同
        重新构造请求
        '''
        try:
            time.sleep(config.crawl_stepWaitTime)
            curpage_pattern_compile = re.compile(r'.*?curpage=(\d+).*?')
            self.get_result_url = CHANGE_PAGE_URL + re.sub(
                curpage_pattern_compile, '?curpage=' + str(self.cur_page_num),
                self.change_page_url)

            if check_ip_valid(self.ip_json):
                get_res = self.session.get(self.get_result_url, headers=HEADER, timeout=13, proxies=self.ip_json)
            else:
                print("主页面 ip 过期: ")
                item.change_global_current_page(self.cur_page_num)  # 可能冗余
                return

            # print("get another page:", download_page_left)
            self.parse_page(download_page_left, get_res.text, item)
        except Exception as e:
            print("get another page fail")

            item.change_global_current_page(self.cur_page_num)  # 可能冗余
            logging.error(e)

        return

    def download_refence(self,url, single_refence_list, item):
        '''
        CAI：注意这是第一个列表页面的CAJ 文献下载，而不是详情页的下载，目前处于关闭状态
        拼接下载地址
        进行文献下载
        '''
        print(self.cur_page_num, '正在下载: '+ item.return_name() + ' ' + single_refence_list[1])
        name = single_refence_list[1] + '_' + single_refence_list[2]
        # 检查文件命名，防止网站资源有特殊字符本地无法保存
        file_pattern_compile = re.compile(r'[\\/:\*\?"<>\|]')
        name = re.sub(file_pattern_compile, '', name)
        # 拼接下载地址
        self.download_url = DOWNLOAD_URL + re.sub(r'../', '', url)
        # 保存下载链接
        # with open('data/Links.txt', 'a', encoding='utf-8') as file:
        #     file.write(self.download_url + '\n')
        # 检查是否开启下载模式
        if config.crawl_isdownload == '1':
            if not os.path.isdir('data/CAJs'):
                os.mkdir(r'data/CAJs')
            refence_file = requests.get(self.download_url, headers=HEADER, timeout=14)

            if not os.path.exists('data/CAJs\\' + name + '.caj'):
                with open('data/CAJs\\' + name + '.caj', 'wb') as file:
                    file.write(refence_file.content)
            else:
                print("Fail! The same name", name)
            time.sleep(config.crawl_stepWaitTime)


def s2h(seconds):
    '''
    将秒数转为小时数
    '''
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    return ("%02d小时%02d分钟%02d秒" % (h, m, s))


def main(word, year, item):
    time.perf_counter()
    # be careful to create a empty data folder!
    # if os.path.isdir('data'):
    #     print(' ')
    #     # 递归删除文件
    #     # shutil.rmtree('data')
    # # 创建一个空的
    # else:
    #     os.mkdir('data')
    if not os.path.exists('data'):
        os.makedirs('data')
    search = SearchTools(item.return_counter())
    search.search_reference(get_uesr_inpt(word, year), item)
    # print('爬虫共运行：' + s2h(time.perf_counter()))
    # print('－－－－－－－－－－－－－－－－－－－－－－－－－－')


class GlobalItem(object):
    def __init__(self, counter, name):
        self.global_current_page = 1
        self.global_first = 1
        self.global_remain_page = 9999
        self.counter = counter
        self.start_page = 1
        self.name = name
        self.get_page_detail = PageDetail()

    def change_global_remain_page(self, nn):
        self.global_remain_page = nn

    def change_global_current_page(self, nn):
        self.global_current_page = nn

    def change_global_first(self, nn):
        self.global_first = nn

    def return_remain_page(self):
        return self.global_remain_page

    def return_current_page(self):
        return self.global_current_page

    def return_counter(self):
        return self.counter

    def return_global_first(self):
        return self.global_first

    def return_start_page(self):
        return self.start_page

    def change_start_page(self, nn):
        self.start_page = nn

    def return_name(self):
        return self.name


class MyTask:
    def __init__(self, page_start, name, counter):
        self.page_start = page_start
        self.name = name
        self.counter = counter

    def run(self):  # 把要执行的代码写到run函数里面 线程在创建后会直接运行run函数
        print("Starting " + self.name)
        # this self.counter is for IP
        item = GlobalItem(self.counter, self.name)
        word = self.name.strip()
        # try:
        #     main(word, item.return_remain_page(), year, item)
        #     while item.return_remain_page() != -1:
        #         item.change_global_first(0)
        #         main(word, item.return_remain_page(), year, item)
        #         # print("出现验证码，将继续下载。剩余的页数:", item.return_remain_page())
        #     # print('－－－－－－－－－－－－－－－－－－－－－－－－－－')
        #     with open('finish_journal.txt','a') as file:
        #         file.write(self.name)
        #         file.write('\n')
        #     print("Exiting, success finish" + self.name)
        # except Exception as e:
        #     logging.error(e)
        #     with open('fail_journal.txt','a') as file:
        #         file.write(self.name)
        #         file.write('\n')
        #     print("Fail " + self.name)
        try:
            main(word, year, item)
        except:
            pass
        # print("first main procedure finish")
        while item.return_remain_page() != -1:
            try:
                # print("将重新登录以继续下载。剩余的页数:", word, item.return_remain_page())
            # print(item.return_current_page())
                item.change_global_first(0)
                main(word, year, item)
            except Exception as e:
                logging.error(e)
                time.sleep(1)

        # print('－－－－－－－－－－－－－－－－－－－－－－－－－－')
        with open('finish_journal.txt', 'a', encoding='utf-8') as file:
            file.write(self.name)
            file.write('\n')
        print("Exiting, success finish" + self.name, item.return_remain_page())

    def return_name(self):
        return self.name


queue = queue.Queue()


# 定义需要线程池执行的任务
def do_job():
    while True:
        i = queue.get()
        # print('index %s, curent: %s' % (i.return_name(), threading.current_thread()))
        i.run()
        queue.task_done()


FAIL = 0
if __name__ == '__main__':
    file = open("test_journal.txt", 'r', encoding='utf-8')
    lines = file.readlines()
    file.close()
    fail_file = open("fail_pdf.txt", "a", encoding='utf-8')
    fail_file.close()
    # year = input("year:")
    # IP = input("IP start:")
    year = input("爬取的年份：")

    number_of_thread = 15
    # 创建包括number_of_thread个线程的线程池
    for i in range(number_of_thread):
        Thread = threading.Thread
        t = Thread(target=do_job)
        t.start()

    # 模拟创建线程池0.1秒后塞进任务到队列
    time.sleep(0.1)
    cnt = 0
    for i in lines:
        # 检查文件命名，防止网站资源有特殊字符本地无法保存
        file_pattern_compile = re.compile(r'[\\/:\*\?"<>\|]')
        journal_name = re.sub(file_pattern_compile, '', i[:-1])
        if not os.path.exists(journal_name):
            os.makedirs(journal_name)
        task = MyTask(0, journal_name, cnt)
        queue.put(task)

        cnt += 1
        if cnt == number_of_thread*2:
            # break
            time.sleep(600)
            cnt = 0
        time.sleep(10)
    queue.join()

