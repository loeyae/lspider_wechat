#-*- coding: utf-8 -*-
# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

__author__="Zhang Yi <loeyae@gmail.com>"
__date__ ="$2019-2-19 9:16:07$"


from setuptools import setup, find_packages

setup(
    name = "cdspider_robot",
    version = "0.1",
    description = "Color-Data数据采集框架机器人",
    author = 'Zhang Yi',
    author_email = 'loeyae@gmail.com',
    license = "Apache License, Version 2.0",
    url="https://github.com/loeyae/lspider_robot.git",
    install_requires = [
        'cdspider>=0.1',
    ],
    packages = find_packages(),

    package_data = {
    },
    entry_points = {
        'cdspider.handler': [
            'default=cdspider.handler:GeneralHandler',
            'list=cdspider.handler:GeneralListHandler',
            'wechat-list=cdspider.handler:WechatListHandler',
            'toutiao-list=cdspider.handler:ToutiaoListHandler',
            'bbs-list=cdspider.handler:BbsListHandler',
            'wemedia-list=cdspider.handler:WemediaListHandler',
            'item=cdspider.handler:GeneralItemHandler',
            'wechat-item=cdspider.handler:WechatItemHandler',
            'toutiao-item=cdspider.handler:ToutiaoItemHandler',
            'bbs-item=cdspider.handler:BbsItemHandler',
            'wemedia-item=cdspider.handler:WemediaItemHandler',
            'links-cluster=cdspider.handler:LinksClusterHandler',
            'weibo=cdspider.handler:WeiboHandler',
            'comment=cdspider.handler:CommentHandler',
            'interact=cdspider.handler:InteractHandler',
            'extend=cdspider.handler:ExtendedHandler',
            'search=cdspider.handler:GeneralSearchHandler',
            'weibo-search=cdspider.handler:WeiboSearchHandler',
            'site-search=cdspider.handler:SiteSearchHandler',
            'wechat-search=cdspider.handler:WechatSearchHandler',
        ],
        'cdspider.dao.mongo': [
            'Base=cdspider.database.mongo.Base',
            'ArticlesDB=cdspider.database.mongo.ArticlesDB',
            'AttachDataDB=cdspider.database.mongo.AttachDataDB',
            'InteractDB=cdspider.database.mongo.InteractDB',
            'ChannelRulesDB=cdspider.database.mongo.ChannelRulesDB',
            'CommentsDB=cdspider.database.mongo.CommentsDB',
            'CrawlLogDB=cdspider.database.mongo.CrawlLogDB',
            'KeywordsDB=cdspider.database.mongo.KeywordsDB',
            'MediaTypesDB=cdspider.database.mongo.MediaTypesDB',
            'ParseRuleDB=cdspider.database.mongo.ParseRuleDB',
            'ProjectsDB=cdspider.database.mongo.ProjectsDB',
            'SitesDB=cdspider.database.mongo.SitesDB',
            'TaskDB=cdspider.database.mongo.TaskDB',
            'UniqueDB=cdspider.database.mongo.UniqueDB',
            'UrlsDB=cdspider.database.mongo.UrlsDB',
            'CommentsUniqueDB=cdspider.database.mongo.CommentsUniqueDB',
            'RepliesUniqueDB=cdspider.database.mongo.RepliesUniqueDB',
            'UrlsUniqueDB=cdspider.database.mongo.UrlsUniqueDB',
            'WechatRobotChatInfoDB=cdspider.database.mongo.WechatRobotChatInfoDB',
            'WechatRobotChatRoomsDB=cdspider.database.mongo.WechatRobotChatRoomsDB',
            'WechatRobotFriendsDB=cdspider.database.mongo.WechatRobotFriendsDB',
            'WechatRobotGroupChatDB=cdspider.database.mongo.WechatRobotGroupChatDB',
            'WechatRobotInfoDB=cdspider.database.mongo.WechatRobotInfoDB',
            'WechatRobotMpsChatDB=cdspider.database.mongo.WechatRobotMpsChatDB',
            'WechatRobotMpsSharingDB=cdspider.database.mongo.WechatRobotMpsSharingDB',
            'WechatRobotMpsDB=cdspider.database.mongo.WechatRobotMpsDB',
            'SpiderTaskDB=cdspider.database.mongo.SpiderTaskDB',
            'ListRuleDB=cdspider.database.mongo.ListRuleDB',
            'CommentRuleDB=cdspider.database.mongo.CommentRuleDB',
            'ExtendRuleDB=cdspider.database.mongo.ExtendRuleDB',
            'AuthorDB=cdspider.database.mongo.AuthorDB',
            'AuthorListRuleDB=cdspider.database.mongo.AuthorListRuleDB',
            'ParseRuleDB=cdspider.database.mongo.ParseRuleDB',
            'RepliesDB=cdspider.database.mongo.RepliesDB',
            'WechatDB=cdspider.database.mongo.WechatDB',
            'WechatRuleDB=cdspider.database.mongo.WechatRuleDB',
            'WeiboAuthorDB=cdspider.database.mongo.WeiboAuthorDB',
            'WeiboInfoDB=cdspider.database.mongo.WeiboInfoDB',
            'ForumRuleDB=cdspider.database.mongo.ForumRuleDB',
            'ErrorLogDB=cdspider.database.mongo.ErrorLogDB',
        ]
    }
)
