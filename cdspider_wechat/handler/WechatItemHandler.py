#-*- coding: utf-8 -*-
# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2018-12-14 23:02:55
"""
import copy
from cdspider.handler import GeneralItemHandler
from cdspider.libs import utils
from cdspider.libs.constants import *
from . import HANDLER_MODE_WECHAT_LIST

class WechatItemHandler(GeneralItemHandler):
    """
    wechat item handler
    :property task 爬虫任务信息 {"mode": "wechat-item", "stid": weixin list spidertask uuid, "url": 微信临时url}
                   or {"mode": "wechat-item", "rid": articles rid}
                   当测试该handler，数据应为 {"mode": "wechat-item", "url": url, "detailRule": 详情规则，参考详情规则}
    """

    def init_process(self, save):
        """
        初始化爬虫流程
        :output self.process {"request": 请求设置, "parse": 解析规则, "paging": 分页规则}
        """
        #根据task中的stid获取任务信息
        stid = self.task.get('stid', None)
        if stid:
            parent_mode = self.task.get('parent-mode', HANDLER_MODE_WECHAT_LIST)
            spiderTask = self.db['SpiderTaskDB'].get_detail(stid, parent_mode)
            if not spiderTask:
                raise CDSpiderHandlerError("list task: %s not exists" % stid)
            crawlinfo = {
                'listMode': spiderTask.get("mode", HANDLER_MODE_WECHAT_LIST),
                "mode": self.mode,  # mode
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

    def run_next(self, task):
        if 'rid' in self.task:
            del self.task['rid']
        crawlinfo = {
                "mode": task['mode'],    # mode
                "stid": task.get("stid", 0),    # SpiderTask uuid
                "uid": self.task['listtask'].get("uid", 0),     # url id
                "pid": self.task['listtask'].get('pid', 0),     # project id
                "sid": self.task['listtask'].get('sid', 0),     # site id
                "tid": self.task['listtask'].get('tid', 0),     # task id
                "list_url": self.task['listtask'].get('save').get('base_url'),  # 列表url
                "list_crawl_id": task.get('crawlid', self.task['listtask']['crawltime']),     # 列表抓取时间
            }
        self.task['crawlinfo'] = crawlinfo
        self.request_params['url'] = task['url']
