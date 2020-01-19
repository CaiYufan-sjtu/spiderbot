"""
-------------------------------------------------
   File Name：     GetPageDetail.py
   Description :   获取文献摘要等信息存储至excel
   Author :        Cai Yufan
   date：          2019/9/30
-------------------------------------------------
   Change Activity:
         增加收集分类号，页号，页数的代码
         增加检测是否下载成功，若不成功，尝试三次
         写入csv文件而不是excel
         增加ip代理模块
-------------------------------------------------
"""
import xlwt
import csv
from bs4 import BeautifulSoup
from GetConfig import config
import re
import math,random
import os
import requests
import time
import logging
HEADER = config.crawl_headers
# 下载的基础链接
DOWNLOAD_URL = 'http://kns.cnki.net'


class PageDetail(object):
    def __init__(self):
        # count用于计数excel行
        self.excel = xlwt.Workbook(encoding='utf8')
        self.sheet = self.excel.add_sheet('文献列表', True)
        self.set_style()
        self.sheet.write(0, 0, '序号',self.basic_style)
        self.sheet.write(0, 1, '题名',self.basic_style)
        self.sheet.write(0, 2, '作者',self.basic_style)
        self.sheet.write(0, 3, '单位',self.basic_style)
        self.sheet.write(0, 4, '关键字',self.basic_style)
        self.sheet.write(0, 5, '摘要',self.basic_style)
        self.sheet.write(0, 6, '来源',self.basic_style)
        self.sheet.write(0, 7, '发表时间',self.basic_style)
        self.sheet.write(0, 8, '数据库',self.basic_style)
        # class_id, page_number, all_page_number
        self.sheet.write(0, 10,'分类号',self.basic_style)
        self.sheet.write(0, 11, '页号', self.basic_style)
        self.sheet.write(0, 12 , '页数', self.basic_style)

        if config.crawl_isDownLoadLink=='1':
            self.sheet.write(0, 9, '下载地址',self.basic_style)


        # 生成userKey,服务器不做验证
        self.cnkiUserKey=self.set_new_guid()

    def download_refence(self,url, single_refence_list):
        '''
        拼接下载地址
        进行文献下载
        '''
        # ('正在下载: ' + single_refence_list[1] + '.caj')
        name = single_refence_list[1] + '_' + single_refence_list[2]
        # 检查文件命名，防止网站资源有特殊字符本地无法保存
        file_pattern_compile = re.compile(r'[\\/:\*\?"<>\|]')
        name = re.sub(file_pattern_compile, '', name)
        # 拼接下载地址
        # print(url)
        if 'kns.cnki.net' in url.strip():
            self.download_url = 'http:' + url.strip()
        else:
            self.download_url = DOWNLOAD_URL + url.strip()
        if 'http://' not in self.download_url:
            self.download_url = 'http://' + self.download_url[6:]
        # print(self.download_url)
        # # 保存下载链接
        # with open('data/Links.txt', 'a', encoding='utf-8') as file:
        #     file.write(self.download_url + '\n')
        # 检查是否开启下载模式，若下载失败，会等待10秒重复尝试3次，若仍无法成功，则下载失败
        flag = 1
        while flag >= 1:
            if not os.path.isdir('data/CAJs'):
                os.mkdir(r'data/CAJs')
            # print(self.download_url, HEADER)
            refence_file = requests.get(self.download_url, headers=HEADER,timeout=15)
            if os.path.exists('data/CAJs\\' + name + '.pdf'):
                if os.path.getsize('data/CAJs\\' + name + '.pdf') > 200:
                    return

            with open('data/CAJs\\' + name + '.pdf', 'wb') as file:
                file.write(refence_file.content)
                
            if os.path.getsize('data/CAJs\\' + name + '.pdf') < 200:
                flag += 1
                # print("由于下载过于频繁，下载"+name+"失败了")
            else:
                flag = 0
            time.sleep(10)
            if flag >= 3:
                with open("fail_pdf.txt", "a", encoding='utf-8') as file:
                    file.write(name+'\n')
                break

    def get_detail_page(self, session, result_url, page_url,
                        single_refence_list, download_url, page_num, name, ip):
        '''
        发送三次请求
        前两次服务器注册 最后一次正式跳转
        '''
        # 这个header必须设置
        HEADER['Referer'] = result_url
        self.single_refence_list = single_refence_list
        self.session = session
        self.session.cookies.set('cnkiUserKey', self.cnkiUserKey)
        self.download_url=download_url
        # 用于不同线程写入不同文件夹以区分不同的期刊
        self.name = name
        cur_url_pattern_compile = re.compile(
            r'.*?FileName=(.*?)&.*?DbCode=(.*?)&')
        cur_url_set=re.search(cur_url_pattern_compile,page_url)
        # print(cur_url_set)
        # 前两次请求需要的验证参数
        params = {
            'curUrl':'detail.aspx?dbCode=' + cur_url_set.group(2) + '&fileName='+cur_url_set.group(1),
            'referUrl': result_url+'#J_ORDER&',
            'cnkiUserKey': self.session.cookies['cnkiUserKey'],
            'action': 'file',
            'userName': '',
            'td': '1544605318654'
        }
        # 首先向服务器发送两次预请求
        self.session.get(
            'http://i.shufang.cnki.net/KRS/KRSWriteHandler.ashx',
            headers=HEADER,
            params=params, timeout=16, proxies=ip)
        self.session.get(
            'http://kns.cnki.net/KRS/KRSWriteHandler.ashx',
            headers=HEADER,
            params=params, timeout=17, proxies=ip)
        page_url = 'http://kns.cnki.net' + page_url
        get_res = self.session.get(page_url, headers=HEADER, timeout=18, proxies=ip)
        self.pars_page(get_res.text, single_refence_list)
        self.excel.save('data/Reference_detail.xls')

    def pars_page(self, detail_page, single_refence_list):
        '''
        解析页面信息
        '''
        soup=BeautifulSoup(detail_page,'lxml')
        
        # 获取作者单位信息
        orgn_list=soup.find(name='div', class_='orgn').find_all('a')
        self.orgn=''
        if len(orgn_list)==0:
            self.orgn='O'
        else:
            for o in orgn_list:
                self.orgn+=o.string

        # 获取链接，下载pdf
        #############################################################################
        # link = soup.find(id='pdfDown')
        # print(link['href'])
        # self.download_refence(link['href'], single_refence_list)
        
        # 获取摘要
        try:
            abstract_list = soup.find(name='span', id='ChDivSummary').strings
            self.abstract=''
            for a in abstract_list:
                self.abstract+=a
        except:
            # print("get abstract fail, maybe no abstract")
            self.abstract='O'

        # 获取关键词
        self.keywords=''
        try:
            keywords_list = soup.find(name='label', id='catalog_KEYWORD').next_siblings
            for k_l in keywords_list:
                # 去除关键词中的空格，换行
                for k in k_l.stripped_strings:
                    self.keywords+=k

        except:
            # print("get keywords fail, maybe no keywords")
            self.keywords='O'

        # class_id, page_number, all_page_number
        self.class_id = ''
        try:
            for i in soup.find(id="catalog_ZTCLS").parent.strings:
                self.class_id = i
            # print("class_id:",self.class_id)
        except:
            # print("no class_id")
            self.class_id = 'O'

        self.page_number = ''
        try:
            # print(soup.find(class_='total').find_all('b'))
            self.page_number = soup.find(class_='total').find_all('b')[1].string
            # print(self.page_number)
        except:
            self.page_number = 'O'
            # print("no page number")

        self.all_page_number = ''
        try:
            self.all_page_number = soup.find(class_='total').find_all('b')[2].string
            # print(self.all_page_number)
        except:
            self.all_page_number = 'O'
            # print("no all page number")

        self.wtire_excel()

    def create_list(self):
        '''
        整理excel每一行的数据
        序号 题名 作者 单位 关键字 摘要  来源 发表时间 数据库
        '''
        self.reference_list = []
        for i in range(0,3):
            self.reference_list.append(self.single_refence_list[i])
        self.reference_list.append(self.orgn)
        self.reference_list.append(self.keywords)
        self.reference_list.append(self.abstract)
        for i in range(3,6):
            self.reference_list.append(self.single_refence_list[i])
        self.reference_list.append(self.class_id)
        self.reference_list.append(self.page_number)
        self.reference_list.append(self.all_page_number)

        if config.crawl_isDownLoadLink == '1':
            self.reference_list.append(self.download_url)

    def wtire_excel(self):
        '''
        将获得的数据写入到excel
        #####################################################
        92 update: excel 不再使用，直接写入csv文件
        '''
        self.create_list()
        # print(self.reference_list)
        out = open(self.name+"/detail.csv", 'a', newline='', encoding='utf-8')
        csv_write = csv.writer(out, dialect='excel')
        csv_write.writerow(self.reference_list)
        '''
        if config.crawl_isDownLoadLink=='1':
            for i in range(0,10):
                self.sheet.write(int(self.reference_list[0]),i,self.reference_list[i],self.basic_style)
        else:
            for i in range(0,9):
                self.sheet.write(int(self.reference_list[0]),i,self.reference_list[i],self.basic_style)
        '''

    def set_style(self):
        '''
        设置excel样式
        '''
        self.sheet.col(1).width = 256 * 30
        self.sheet.col(2).width = 256 * 15
        self.sheet.col(3).width = 256 * 20
        self.sheet.col(4).width = 256 * 20
        self.sheet.col(5).width = 256*60
        self.sheet.col(6).width = 256 * 15
        self.sheet.col(9).width = 256 * 15
        self.sheet.col(10).width = 256 * 15
        self.sheet.col(11).width = 256 * 15
        self.sheet.col(12).width = 256 * 15
        self.sheet.row(0).height_mismatch=True
        self.sheet.row(0).height = 20*20
        self.basic_style=xlwt.XFStyle()
        al=xlwt.Alignment()
        # 垂直对齐
        al.horz = al.HORZ_CENTER
        # 水平对齐
        al.vert =al.VERT_CENTER
        # 换行
        al.wrap = al.WRAP_AT_RIGHT
        # 设置边框
        borders = xlwt.Borders()
        borders.left = 6
        borders.right = 6
        borders.top = 6
        borders.bottom = 6

        self.basic_style.alignment=al
        self.basic_style.borders=borders

    def set_new_guid(self):
        '''
        生成用户秘钥
        '''
        guid=''
        for i in range(1,32):
            n = str(format(math.floor(random.random() * 16.0),'x'))
            guid+=n
            if (i == 8) or (i == 12) or (i == 16) or (i == 20):
                guid += "-"
        return guid

