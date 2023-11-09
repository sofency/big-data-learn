package com.sofency.top.bean

import com.alibaba.fastjson2.JSONObject

/**
 *
 * <p>Project: big-data-learn - Common
 * <p>Powered by echo On 2023-11-02 20:33:12
 *
 * @author sofency [sofency@qq.com]
 * @version 1.0
 * @since 8
 */

/**
 *
 * @param province_id    区域id
 * @param user_id        用户id
 * @param operate_system 操作系统
 * @param channel        渠道
 * @param isNew          是否为新用户
 * @param mode           设备类型
 * @param mid            设备型号
 * @param version_code   版本
 * @param brand          品牌
 */
case class Common(province_id: String, user_id: String, operate_system: String, channel: String, isNew: String, mode: String, mid: String, version_code: String, brand: String)

/**
 *
 * @param duringTime 停留时间
 * @param item       商品号
 * @param itemType   商品类型
 * @param lastPageId 上一页面id
 * @param pageId     当前页面id
 * @param sourceType 来源类型
 */
case class Page(duringTime: Long, item: String, itemType: String, lastPageId: String, pageId: String, sourceType: String)

case class Start(entry: String, openAdSkipMs: Long, openAdMs: Long, loadingTime: Long, openAdId: Long)

case class Displays(displayType: String, item: String, itemType: String, order: String, posId: String)

case class Actions(actionId: String, item: String, itemType: String)

/**
 *
 * @param province_id    区域id
 * @param user_id        用户id
 * @param operate_system 操作系统
 * @param channel        渠道
 * @param isNew          是否为新用户
 * @param model          设备类型
 * @param mid            设备型号
 * @param version_code   版本
 * @param brand          品牌
 * @param duringTime     停留时间
 * @param item           商品号
 * @param itemType       商品类型
 * @param lastPageId     上一页面id
 * @param pageId         页面id
 * @param sourceType     来源类型
 * @param ts             时间戳
 */
case class CommonPage(province_id: String, user_id: String, operate_system: String, channel: String, isNew: String, model: String, mid: String,
                      version_code: String, brand: String, duringTime: Long, item: String, itemType: String, lastPageId: String, pageId: String, sourceType: String,
                      ts: Long)

/**
 *
 * @param province_id
 * @param user_id
 * @param operate_system
 * @param channel
 * @param isNew
 * @param model
 * @param mid
 * @param version_code
 * @param entry
 * @param openAdSkipMs
 * @param openAdMs
 * @param loadingTime
 * @param openAdId
 * @param ts
 */
case class CommonStart(province_id: String, user_id: String, operate_system: String, channel: String, isNew: String, model: String, mid: String,
                       version_code: String, entry: String, openAdSkipMs: Long, openAdMs: Long, loadingTime: Long, openAdId: Long, ts: Long)

/**
 *
 * @param province_id       区域id
 * @param user_id           用户id
 * @param operate_system    操作系统
 * @param channel           渠道
 * @param isNew             是否为新用户
 * @param model             设备类型
 * @param mid               设备型号
 * @param version_code      版本
 * @param brand             品牌
 * @param duringTime        停留时间
 * @param page_id           停留时间
 * @param last_page_id      停留时间
 * @param page_item         页面物品
 * @param page_item_type    页面物品类型
 * @param display_item      商品号
 * @param display_item_type 商品类型
 * @param display_order     上一页面id
 * @param display_pos_id    页面id
 * @param sourceType        来源类型
 * @param ts                时间戳
 */
case class CommonDisplays(province_id: String, user_id: String, operate_system: String, channel: String, isNew: String, model: String, mid: String,
                          version_code: String, brand: String,
                          duringTime: Long, page_id: String, last_page_id: String, page_item: String, page_item_type: String,
                          display_item: String, display_item_type: String, display_order: String, display_pos_id: String, sourceType: String,
                          ts: Long)

case class CommonActions(province_id: String, user_id: String, operate_system: String, channel: String, isNew: String, model: String, mid: String,
                         version_code: String, brand: String,
                         duringTime: Long, page_id: String, last_page_id: String, page_item: String, page_item_type: String,
                         action_id: String, action_item: String, action_item_type: String, sourceType: String,
                         ts: Long)


/**
 * 整合common
 *
 * @param commonObj common对象
 * @return
 */
def parseCommon(commonObj: JSONObject): Common = {
  Common(commonObj.getString("ar"),
    commonObj.getString("uid"),
    commonObj.getString("os"),
    commonObj.getString("ch"),
    commonObj.getString("is_new"),
    commonObj.getString("md"),
    commonObj.getString("mid"),
    commonObj.getString("vc"), commonObj.getString("ba"))
}

/**
 *
 * @param pageObj page页面
 * @return
 */
def parsePage(pageObj: JSONObject): Page = {
  Page(pageObj.getLong("during_time"),
    pageObj.getString("item"),
    pageObj.getString("item_type"),
    pageObj.getString("last_page_id"),
    pageObj.getString("page_id"),
    pageObj.getString("source_type"))
}

/**
 *
 * @param common common对象
 * @param page   page 对象
 * @param ts     时间戳
 * @return
 */
def constructCommonPage(common: Common, page: Page, ts: Long): CommonPage = {
  CommonPage(common.province_id, common.user_id, common.operate_system, common.channel, common.isNew, common.mode, common.mid, common.version_code, common.brand,
    page.duringTime, page.item, page.itemType, page.lastPageId, page.pageId, page.sourceType, ts)
}


/**
 * 获取曝光对象
 *
 * @param displays 曝光
 * @return
 */
def parseDisplays(displays: JSONObject): Displays = {
  Displays(
    displays.getString("display_type"),
    displays.getString("item"),
    displays.getString("item_type"),
    displays.getString("order"),
    displays.getString("pos_id")
  )
}

/**
 *
 * @param common common对象
 * @param page   page 对象
 * @param ts     时间戳
 * @return
 */
def constructCommonDisplays(common: Common, page: Page, displays: Displays, ts: Long): CommonDisplays = {
  CommonDisplays(common.province_id, common.user_id, common.operate_system, common.channel, common.isNew, common.mode, common.mid, common.version_code, common.brand,
    page.duringTime, page.pageId, page.lastPageId, page.item, page.itemType,
    displays.item, displays.itemType, displays.order, displays.posId, page.sourceType, ts)
}


def parseActions(actions: JSONObject): Actions = {
  Actions(
    actions.getString("action_id"),
    actions.getString("item"),
    actions.getString("item_type")
  )
}

def constructCommonActions(common: Common, page: Page, actions: Actions, ts: Long): CommonActions = {
  CommonActions(common.province_id, common.user_id, common.operate_system, common.channel, common.isNew, common.mode, common.mid, common.version_code, common.brand,
    page.duringTime, page.pageId, page.lastPageId, page.item, page.itemType,
    actions.actionId, actions.item, actions.itemType, page.sourceType, ts)
}


def parseStart(startJson: JSONObject): Start = {
  Start(startJson.getString("entry"),
    startJson.getLong("open_ad_skip_ms"),
    startJson.getLong("open_ad_ms"),
    startJson.getLong("loading_time"),
    startJson.getString("open_ad_id"))
}

def constructCommonStart(common: Common, start: Start, ts: Long): CommonStart = {
  CommonStart(common.province_id, common.user_id, common.operate_system, common.channel, common.isNew, common.mode, common.mid, common.version_code,
    start.entry, start.openAdSkipMs, start.openAdMs, start.loadingTime, start.openAdId, ts)
}
