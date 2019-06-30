#-*- coding: utf-8 -*-
# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-12-13 20:37:03
"""
import copy
import time
import traceback
import html
from cdspider.handler import Loader
from cdspider_wemedia.handler import WemediaListHandler
from urllib.parse import urljoin
from cdspider.libs import utils
from cdspider.libs.constants import *
from cdspider.parser import ListParser, CustomParser
from . import HANDLER_MODE_WECHAT_ITEM

class WechatListHandler(WemediaListHandler):
    """
    wechat list handler
    :property task 爬虫任务信息 {"mode": "wechat-list", "uuid": SpiderTask.author uuid}
                   当测试该handler，数据应为 {"mode": "wechat-list", "wxauthorRule": 公众号搜索解析, "author": 自媒体号设置，参考自媒体号, "authorListRule": 自媒体列表规则，参考自媒体列表规则}
    """

    EXPIRE = 7200

    def prepare(self, save):
        super(WechatListHandler, self).prepare(save)
        if not 'save' in self.task or not self.task['save']:
            self.task['save'] = {}
        ctime = self.task['save'].get('timestamp', 0)
        if not self.task['save'].get('request_url') or self.crawl_id - int(ctime) > self.EXPIRE:
            self.proxy_mode = self.process['prepare'].get('request', {}).get('proxy', 'never')
            self.precrawl(save)
            params = copy.deepcopy(self.request_params)
            proxy_mode = self.proxy_mode
            self.crawler.crawl(**params)
            self.debug("%s prepare crawl result: %s" % (self.__class__.__name__, utils.remove_whitespace(self.crawler.page_source)))
            parser = CustomParser(source=self.crawler.page_source, ruleset=copy.deepcopy(self.process['prepare']['parse']))
            accountInfo = parser.parse()
            self.debug("%s prepare parsed: %s" % (self.__class__.__name__, accountInfo))
            only_account = self.task.get('only_account', False)
            if only_account:
                if not accountInfo or not accountInfo[0]['url']:
                    if u"noresult_part1_container" in self.crawler.page_source:
                        save['status'] = 0
                    else:
                        save['status'] = -1
                else:
                    self.response['parsed'] = accountInfo[0]
                    save['status'] = 1
                raise CDSpiderCrawlerReturnBroken()
            else:
                if not accountInfo or not accountInfo[0]['url']:
                    if u"noresult_part1_container" in self.crawler.page_source:
                        self.db['SpiderTaskDB'].disable(self.task['uuid'], self.task['mode'])
                        raise CDSpiderCrawlerNoExists("Wechat account not found")
                    raise CDSpiderCrawlerForbidden()
            save['timestamp'] = self.crawl_id
            self.request_params['url'] = html.unescape(accountInfo[0]['url'])
            self.proxy_mode = proxy_mode
        else:
            self.request_params['url'] = self.task['save'].get('request_url')
        save['base_url'] = self.request_params['url']

    def run_parse(self, rule):
        """
        根据解析规则解析源码，获取相应数据
        :param rule 解析规则
        :input self.response 爬虫结果 {"last_source": 最后一次抓取到的源码, "final_url": 最后一次请求的url}
        :output self.response {"parsed": 解析结果}
        """
        parser = ListParser(source=self.response['last_source'], ruleset=copy.deepcopy(rule), log_level=self.log_level, url=self.response['final_url'])
        parsed = parser.parse()
        if not parsed:
            raise CDSpiderCrawlerForbidden()
        self.response['parsed'] = parsed

    def update_crawl_info(self, save):
        """
        构造文章数据的爬虫信息
        """
        crawlinfo = self.task.get('crawlinfo', {}) or {}
        self.crawl_info['crawl_end'] = int(time.time())
        crawlinfo[str(self.crawl_id)] = self.crawl_info
        crawlinfo_sorted = [(k, crawlinfo[k]) for k in sorted(crawlinfo.keys())]
        if len(crawlinfo_sorted) > self.CRAWL_INFO_LIMIT_COUNT:
            del crawlinfo_sorted[0]
        s = self.task.get("save")
        if not s:
            s = {}
        s.update(save)
        self.db['SpiderTaskDB'].update(self.task['uuid'], self.task['mode'], {"crawltime": self.crawl_id, "crawlinfo": dict(crawlinfo_sorted), "save": save})


    def run_result(self, save):
        """
        爬虫结果处理
        :param save 保存的上下文信息
        :input self.response {"parsed": 解析结果, "final_url": 请求的url}
        """
        self.crawl_info['crawl_urls'][str(self.page)] = self.response['last_url']
        self.crawl_info['crawl_count']['page'] += 1
        self.crawl_info['crawl_count']['total'] = len(self.response['parsed'])
        self.update_crawl_info(save)
        save['update_crawlinfo'] = True
        if self.response['parsed']:
            #格式化url
            item_save = {"base_url": self.response['last_url']}
            formated = self.build_url_by_rule(self.response['parsed'], self.response['final_url'])
            item_handler = Loader(self.ctx, task = self.build_item_task(self.response['last_url']), no_sync = self.no_sync).load()
            item_handler.init(item_save)
            for item in formated:
                try:
                    item_task = self.build_item_task(item['url'])
                    item_handler.run_next(item_task)
                    item_save['retry'] = 0
                    while True:
                        self.info('Item Spider crawl start')
                        item_handler.crawl(item_save)
                        if isinstance(item_handler.response['broken_exc'], CONTINUE_EXCEPTIONS):
                            item_handler.on_continue(item_handler.response['broken_exc'], item_save)
                            continue
                        elif item_handler.response['broken_exc']:
                            raise item_handler.response['broken_exc']
                        if not item_handler.response['last_source']:
                            raise CDSpiderCrawlerError('Item Spider crawl failed')
                        self.info("Item Spider crawl end, source: %s" % utils.remove_whitespace(item_handler.response["last_source"]))
                        self.info("Item Spider parse start")
                        item_handler.parse()
                        self.info("Item Spider parse end, result: %s" % str(item_handler.response["parsed"]))
                        self.info("Item Spider result start")
                        item_handler.on_result(save)
                        self.info("Item Spider result end")
                        break
                except Exception as e:
                    self.on_error(e, save)
            if item_handler:
                item_handler.finish(item_save)

    def build_item_task(self, url):
        """
        生成详情抓取任务并入队
        """
        message = {
            'mode': HANDLER_MODE_WECHAT_ITEM,
            'url': url,
            'crawlid': self.crawl_id,
            'stid': self.task['uuid'],
        }
        return message
#        self.queue['scheduler2spider'].put_nowait(message)

    def finish(self, save):
        """
        记录抓取日志
        """
        super(WechatListHandler, self).finish(save)
        _u = save.pop('update_crawlinfo', False)
        if not _u:
            self.update_crawl_info(save)
        else:
            s = self.task.get("save")
            if not s:
                s = {}
            s.update(save)
            self.db['SpiderTaskDB'].update(self.task['uuid'], self.task['mode'], {"crawltime": self.crawl_id, "save": s})
