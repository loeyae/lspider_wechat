#-*- coding: utf-8 -*-
# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-12-14 23:02:55
"""
import time
import copy
from cdspider.handler import BaseHandler
from cdspider_extra.cdspider_extra.handler.traite import NewAttachmentTask
from cdspider.database.base import *
from cdspider.libs import utils
from cdspider.libs.constants import *
from cdspider.parser import ItemParser
from cdspider.parser.lib import TimeParser

class WechatItemHandler(BaseHandler, NewAttachmentTask):
    """
    wechat item handler
    :property task 爬虫任务信息 {"mode": "wechat-item", "stid": weixin list spidertask uuid, "url": 微信临时url}
                   or {"mode": "wechat-item", "rid": articles rid}
                   当测试该handler，数据应为 {"mode": "wechat-item", "url": url, "detailRule": 详情规则，参考详情规则}
    """

    def route(self, mode, rate, save):
        yield None

    def get_scripts(self):
        """
        获取自定义脚本
        """
        try:
            rule = self.match_rule()
            return rule.get("scripts", None)
        except:
            return None

    def init_process(self, save):
        """
        初始化爬虫流程
        :output self.process {"request": 请求设置, "parse": 解析规则, "paging": 分页规则}
        """
        if "detailRule" in self.task:
            typeinfo = utils.typeinfo(self.task['url'])
            if typeinfo['domain'] != self.task['detailRule']['domain'] or typeinfo['subdomain'] != self.task['detailRule']['subdomain']:
                raise CDSpiderNotUrlMatched()
            if  'urlPattern' in self.task['detailRule'] and self.task['detailRule']['urlPattern']:
                '''
                如果规则中存在url匹配规则，则进行url匹配规则验证
                '''
                u = utils.preg(url, item['urlPattern'])
                if not u:
                    raise CDSpiderNotUrlMatched()
        self.process = self.match_rule()
        if not self.process:
            self.process = {"parse": {}, "unique": {}}
        self.process['parse'].update({
            "__biz": {
                "filter": '@reg:var\s+biz\s*=\s*""\s*\|\|\s*"[biz]";',
            },
            "mid": {
                "filter": '@reg:var\s+mid\s*=\s*""\s*\|\|\s*""\|\|\s*"[mid]";'
            },
            "idx": {
                "filter": '@reg:var\s+idx\s*=\s*""\s*\|\|\s*""\s*\|\|\s*"[idx]";'
            }
        })
        self.process['unique'].update({
            "url": "[url]\?.+",
            "data": "__biz,mid,idx"
        })

    def match_rule(self):
        """
        匹配详情页规则
        """
        #优先获取task中详情规则
        parse_rule = self.task.get("detailRule", {})
        #根据task中的stid获取任务信息
        stid = self.task.get('stid', None)
        if stid:
            parent_mode = self.task.get('parent-mode', HANDLER_MODE_WECHAT_LIST)
            spiderTask = self.db['SpiderTaskDB'].get_detail(stid, parent_mode)
            if not spiderTask:
                raise CDSpiderHandlerError("list task: %s not exists" % stid)
            crawlinfo = {
                'listMode': spiderTask.get("mode", HANDLER_MODE_WECHAT_LIST),
                "mode": HANDLER_MODE_WECHAT_ITEM,  # mode
                "stid": self.task.get("stid", 0),   # SpiderTask uuid
                "uid": spiderTask.get("uid", 0),     # url id
                "pid": spiderTask.get('pid', 0),     # project id
                "sid": spiderTask.get('sid', 0),     # site id
                "tid": spiderTask.get('tid', 0),     # task id
                "kid": spiderTask.get('kid', 0),     # keyword id
                "listRule": spiderTask.get('rid', 0),   # 规则ID
                "list_url": spiderTask.get('save').get('base_url'),  # 列表url
                "list_crawl_id": self.task.get('crawlid', spiderTask['crawltime']),     # 列表抓取时间
            }
            self.task.setdefault('mediaType', MEDIA_TYPE_WECHAT)
            self.task.setdefault('listtask', spiderTask)
            self.task.setdefault('crawlinfo', crawlinfo)
            if 'crawlid' in self.task:
                self.crawl_info = spiderTask['crawlinfo'][str(self.task['crawlid'])]
        elif self.task.get('rid', None):
            rid = self.task.get('rid', None)
            article = self.db['ArticlesDB'].get_detail(rid, select=['url', 'crawlinfo'])
            if not article:
                raise CDSpiderHandlerError("aritcle: %s not exists" % rid)
            if not 'ulr' in self.task or not self.task['url']:
                self.task["url"] = article['url']
            self.task.setdefault('crawlinfo', article.get('crawlinfo', {}))
        else:
            self.task.setdefault('crawlinfo', {})
        url = self.task.get("url")
        if not url:
            raise CDSpiderHandlerError("url not exists")
        if not parse_rule:
            '''
            task中不存在详情规则，根据域名匹配规则库中的规则
            '''
            subdomain, domain = utils.parse_domain(url)
            if subdomain:
                '''
                优先获取子域名对应的规则
                '''
                parserule_list = self.db['ParseRuleDB'].get_list_by_subdomain(subdomain)
                for item in parserule_list:
                    if not parse_rule:
                        '''
                        将第一条规则选择为返回的默认值
                        '''
                        parse_rule = item
                    if  'urlPattern' in item and item['urlPattern']:
                        '''
                        如果规则中存在url匹配规则，则进行url匹配规则验证
                        '''
                        u = utils.preg(url, item['urlPattern'])
                        if u:
                            return item
            else:
                '''
                获取域名对应的规则
                '''
                parserule_list = self.db['ParseRuleDB'].get_list_by_domain(domain)
                for item in parserule_list:
                    if not parse_rule:
                        '''
                        将第一条规则选择为返回的默认值
                        '''
                        parse_rule = item
                    if  'urlPattern' in item and item['urlPattern']:
                        '''
                        如果规则中存在url匹配规则，则进行url匹配规则验证
                        '''
                        u = utils.preg(url, item['urlPattern'])
                        if u:
                            return item
        return parse_rule

    def run_parse(self, rule):
        """
        文章解析
        :param rule 解析规则
        :input self.response 爬虫抓取结果 {"last_source": 最后一次抓取到的源码, "final_url": 最后一次请求的url}
        :output self.response {"parsed": 解析结果}
        """
        parser = ItemParser(source=self.response['last_source'], ruleset=copy.deepcopy(rule), log_level=self.log_level, url=self.response['final_url'])
        self.response['parsed'] = parser.parse()

    def _build_crawl_info(self, final_url):
        """
        构造爬虫log信息
        :param final_url 请求的url
        :input self.task 爬虫任务信息
        :input self.page 当前的页码
        """
        self.task['crawlinfo']['mode'] = HANDLER_MODE_WECHAT_ITEM
        if not 'final_url' in self.task['crawlinfo']:
            self.task['crawlinfo']['final_url'] = {str(self.page): final_url}
        else:
            self.task['crawlinfo']['final_url'][str(self.page)] = final_url
        if not 'detailRule' in self.task['crawlinfo']:
            self.task['crawlinfo']['detailRule'] = self.process.get('uuid', 0)
        self.task['crawlinfo']['page'] = self.page

    def _build_result_info(self, **kwargs):
        """
        构造文章数据
        :param result 解析到的文章信息 {"title": 标题, "author": 作者, "pubtime": 发布时间, "content": 内容}
        :param final_url 请求的url
        :param typeinfo 域名信息 {'domain': 一级域名, 'subdomain': 子域名}
        :param crawlinfo 爬虫信息
        :param unid 文章唯一索引
        :param ctime 抓取时间
        :param status 状态
        """
        now = int(time.time())
        result = kwargs.pop('result')
        #格式化发布时间
        pubtime = TimeParser.timeformat(str(result.pop('pubtime', '')))
        if pubtime and pubtime > now:
            pubtime = now
        r = {
            'mediaType': MEDIA_TYPE_WECHAT,
            "status": kwargs.get('status', ArticlesDB.STATUS_ACTIVE),
            'url': kwargs['final_url'],
            'title': result.pop('title', None),                                # 标题
            'author': result.pop('author', None),                              # 作者
            'content': result.pop('content', None),
            'pubtime': pubtime,                                                # 发布时间
            'channel': result.pop('channel', None),                            # 频道信息
            'result': result,
            'crawlinfo': kwargs.get('crawlinfo')
        }
        if all((r['title'], r['author'], r['content'], r['pubtime'])):
            '''
            判断文章是否解析完全
            '''
            r['status'] = ArticlesDB.STATUS_PARSED
        if "unid" in kwargs:
            r['acid'] = kwargs['unid']                                         # unique str
        if "ctime" in kwargs:
            r['ctime'] = kwargs['ctime']
        if "typeinfo" in kwargs:
            r['domain'] = kwargs.get("typeinfo", {}).get('domain', None)          # 站点域名
            r['subdomain'] = kwargs.get("typeinfo", {}).get('subdomain', None)    # 站点域名
        return r

    def run_next(self, task):
        if 'rid' in self.task:
            del self.task['rid']
        crawlinfo = {
                "mode": HANDLER_MODE_WECHAT_ITEM,    # mode
                "stid": self.task.get("stid", 0),    # SpiderTask uuid
                "uid": self.task['listtask'].get("uid", 0),     # url id
                "pid": self.task['listtask'].get('pid', 0),     # project id
                "sid": self.task['listtask'].get('sid', 0),     # site id
                "tid": self.task['listtask'].get('tid', 0),     # task id
                "list_url": self.task['listtask'].get('save').get('base_url'),  # 列表url
                "list_crawl_id": self.task.get('crawlid', self.task['listtask']['crawltime']),     # 列表抓取时间
            }
        self.task['crawlinfo'] = crawlinfo
        self.request_params['url'] = task['url']

    def run_result(self, save):
        """
        爬虫结果处理
        :param save 保存的上下文信息
        """
        self._build_crawl_info(final_url=self.response['final_url'])
        if self.response['parsed']:
            typeinfo = utils.typeinfo(self.response['final_url'])
            self.result2db(save, copy.deepcopy(typeinfo))

    def result2db(self, save, typeinfo):
        """
        详情解析结果入库
        :param save 保存的上下文信息
        :param typeinfo 域名信息
        """
        result_id = self.task.get("rid", None)
        ctime = self.crawl_id
        if not result_id:
            '''
            如果任务中没有文章id，则生成文章唯一索引，并判断是否已经存在。
            如果文章已经存在，则修改原数据，如果不存在，则新增数据
            '''
            if self.testing_mode:
                '''
                testing_mode打开时，数据不入库
                '''
                inserted, unid = (True, {"acid": "testing_mode", "ctime": self.crawl_id})
                self.debug("%s test mode: %s" % (self.__class__.__name__, unid))
            else:
                #生成文章唯一索引并验证是否已存在
                inserted, unid = self.db['UniqueDB'].insert(self.get_unique_setting(self.response['final_url'], self.response['parsed']), ctime)
                self.debug("%s on_result unique: %s @ %s" % (self.__class__.__name__, str(inserted), str(unid)))
            #格式化文章信息
            result = self._build_result_info(final_url=self.request_params['url'], typeinfo=typeinfo, result=self.response['parsed'], crawlinfo=self.task['crawlinfo'], **unid)
            if self.testing_mode:
                '''
                testing_mode打开时，数据不入库
                '''
                result_id = 'testing_mode'
                self.debug("%s on_result: %s" % (self.__class__.__name__, result))
            else:
                self.debug("%s on_result formated data: %s" % (self.__class__.__name__, str(result)))
                if inserted:
                    result_id = self.db['ArticlesDB'].insert(result)
                    self.build_sync_task(result_id, 'ArticlesDB')
                    if not result_id:
                        raise CDSpiderDBError("Result insert failed")
                    self.task['crawlinfo'] = result['crawlinfo']
                    self.task['rid'] = result_id
                    self.crawl_info['crawl_count']['new_count'] += 1
                else:
                    self.crawl_info['crawl_count']['repeat_count'] += 1
            if inserted:
                self.result2attach(save, **typeinfo)
        else:
            if self.page == 1:
                '''
                对于已存在的文章，如果是第一页，则更新所有解析到的内容
                否则只追加content的内容
                '''
                #格式化文章信息
                result = self._build_result_info(final_url=self.response['final_url'], typeinfo=typeinfo, result=self.response['parsed'], crawlinfo=self.task['crawlinfo'])

                if self.testing_mode:
                    '''
                    testing_mode打开时，数据不入库
                    '''
                    self.debug("%s on_result: %s" % (self.__class__.__name__, result))
                else:
                    self.debug("%s on_result formated data: %s" % (self.__class__.__name__, str(result)))
                    self.db['ArticlesDB'].update(result_id, result)
                    self.build_sync_task(result_id, 'ArticlesDB')
                self.result2attach(save, **typeinfo)
            else:
                if self.testing_mode:
                    '''
                    testing_mode打开时，数据不入库
                    '''
                    self.debug("%s on_result: %s" % (self.__class__.__name__, self.response['parsed']))
                else:
                    result = self.db['ArticlesDB'].get_detail(result_id)
                    content = result['content']
                    if 'content' in self.response['parsed'] and self.response['parsed']['content']:
                        content = '%s\r\n\r\n%s' % (content, self.response['parsed']['content'])
                        self.debug("%s on_result content: %s" % (self.__class__.__name__, content))
                        self.db['ArticlesDB'].update(result_id, {"content": content})
                        self.build_sync_task(result_id, 'ArticlesDB')

    def finish(self, save):
        """
        记录抓取日志
        """
        super(WechatItemHandler, self).finish(save)
        if self.task.get('rid') and self.task.get('crawlinfo') and not self.testing_mode:
            self.db['ArticlesDB'].update(self.task['rid'], {"crawlinfo": self.task['crawlinfo']})
            self.build_sync_task(self.task['rid'], 'ArticlesDB')
        if self.task.get('stid') and self.task.get('crawlid') and not self.testing_mode:
            if self.crawl_info['crawl_count']['repeat_count'] == self.crawl_info['crawl_count']['total']:
                self.crawl_info['crawl_count']['repeat_page'] += 1
            parent_mode = self.task.get('parent-mode', HANDLER_MODE_WECHAT_LIST)
            self.db['SpiderTaskDB'].update(self.task['stid'], parent_mode, {"crawlinfo.%s" % str(self.task['crawlid']): self.crawl_info})

    def build_sync_task(self, rid, db):
        """
        生成同步任务并入队
        """
        message = {'rid': rid, 'db': db}
        self.queue['article2kafka'].put_nowait(message)
