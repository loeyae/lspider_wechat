#-*- coding: utf-8 -*-
# Licensed under the Apache License, Version 2.0 (the "License"),
# see LICENSE for more details: http://www.apache.org/licenses/LICENSE-2.0.

"""
:author:  Zhang Yi <loeyae@gmail.com>
:date:    2019/4/10 10:29
"""

HANDLER_MODE_WECHAT_LIST = "wechat-list"
HANDLER_MODE_WECHAT_SEARCH = "wechat-search"
HANDLER_MODE_WECHAT_ITEM = "wechat-item"

from .WechatSearchHandler import WechatSearchHandler
from .WechatListHandler import WechatListHandler
from .WechatItemHandler import WechatItemHandler