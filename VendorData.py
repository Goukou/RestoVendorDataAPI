# -*- coding: utf-8 -*-
"""
脚本名称: Resto开放平台供应商数据同步脚本
功能描述: 
    1. 自动在数据库创建供应商相关表结构 (主表、账户表、税率表)。
    2. 分页调用 Resto 开放平台接口 querySuppliers 获取供应商数据。
    3. 解析复杂的嵌套 JSON 结构 (联系人、财务、账户、税率)。
    4. 将数据清洗并入库 (支持增量更新/覆盖)。

依赖库: requests, pymysql
Python版本: 3.6+
"""

import requests
import json
import hashlib
import time
import uuid
import datetime
import pymysql
from pymysql.cursors import DictCursor

# ==============================================================================
# 1. 配置区域 (请在此处修改您的实际配置)
# ==============================================================================

# --- 数据库连接配置 ---
DB_CONFIG = {
    'host': 'null',       # 数据库地址
    'port': null,              # 数据库端口
    'user': 'null',         # 数据库用户名
    'password': 'null',    # 数据库密码 [请修改此处]
    'database': 'null', # 目标数据库名
    'charset': 'utf8mb4'       # 字符集，推荐 utf8mb4 以支持特殊符号
}

# --- 开放平台 API 配置 ---
API_CONFIG = {
    'host': 'null',                          # API 域名
    'path': 'null',              # 供应商查询接口路径
    'app_key': 'null',                                # 您的 AppKey
    'secret_key': 'null',       # 您的 SecretKey
    'corporation_id': null,                              # 租户ID (集团ID)
    #'org_code': 0                              # 店铺ID

}

# ==============================================================================
# 2. 数据库初始化 (DDL)
# ==============================================================================

def init_database(connection):
    """
    检查并初始化数据库表结构。
    """
    print("[-] 正在检查并初始化数据库表结构...")
    cursor = connection.cursor()

    # 1. 供应商主表
    sql_main = """
    CREATE TABLE IF NOT EXISTS `supplier_main` (
      `id` BIGINT(20) NOT NULL COMMENT '供应商ID (主键)',
      `supplier_code` VARCHAR(64) DEFAULT NULL COMMENT '供应商编码',
      `supplier_name` VARCHAR(128) DEFAULT NULL COMMENT '供应商名称',
      `short_name` VARCHAR(64) DEFAULT NULL COMMENT '简称',
      `is_enabled` TINYINT(1) DEFAULT NULL COMMENT '状态(1启用 0禁用)',
      `category_code` VARCHAR(64) DEFAULT NULL COMMENT '所属分类编码',
      `category_name` VARCHAR(128) DEFAULT NULL COMMENT '所属分类名称',
      
      `contact_name` VARCHAR(64) DEFAULT NULL COMMENT '联系人姓名',
      `contact_phone` VARCHAR(32) DEFAULT NULL COMMENT '联系电话',
      `email` VARCHAR(128) DEFAULT NULL COMMENT '邮箱',
      `region_name` VARCHAR(64) DEFAULT NULL COMMENT '国家/地区',
      `province_name` VARCHAR(64) DEFAULT NULL COMMENT '省',
      `city_name` VARCHAR(64) DEFAULT NULL COMMENT '市',
      `district_name` VARCHAR(64) DEFAULT NULL COMMENT '区',
      `address_detail` VARCHAR(255) DEFAULT NULL COMMENT '详细地址(拼接 街道+门牌)',
      
      `tax_entity_name` VARCHAR(128) DEFAULT NULL COMMENT '纳税主体',
      `tax_identification_no` VARCHAR(64) DEFAULT NULL COMMENT '企业税号',
      
      `sync_time` DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '数据同步时间',
      PRIMARY KEY (`id`),
      KEY `idx_supplier_code` (`supplier_code`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='供应商主表';
    """

    # 2. 供应商账户信息表
    sql_account = """
    CREATE TABLE IF NOT EXISTS `supplier_account` (
      `id` INT AUTO_INCREMENT PRIMARY KEY,
      `supplier_id` BIGINT(20) NOT NULL COMMENT '关联主表供应商ID',
      `account_name` VARCHAR(128) DEFAULT NULL COMMENT '账户名称',
      `account_no` VARCHAR(64) DEFAULT NULL COMMENT '银行账号',
      `channel_name` VARCHAR(128) DEFAULT NULL COMMENT '银行/渠道名称',
      `is_default` TINYINT(1) DEFAULT NULL COMMENT '是否默认(0否 1是)',
      KEY `idx_supplier_id` (`supplier_id`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='供应商账户信息表';
    """

    # 3. 供应商税率信息表
    sql_tax = """
    CREATE TABLE IF NOT EXISTS `supplier_tax` (
      `id` INT AUTO_INCREMENT PRIMARY KEY,
      `supplier_id` BIGINT(20) NOT NULL COMMENT '关联主表供应商ID',
      `tax_name` VARCHAR(64) DEFAULT NULL COMMENT '税种名称',
      `tax_code` VARCHAR(64) DEFAULT NULL COMMENT '税种编码',
      `tax_type` INT(11) DEFAULT NULL COMMENT '税种类型(1:按价 2:按量)',
      `tax_value` VARCHAR(32) DEFAULT NULL COMMENT '税值',
      KEY `idx_supplier_id` (`supplier_id`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='供应商税率信息表';
    """

    try:
        cursor.execute(sql_main)
        cursor.execute(sql_account)
        cursor.execute(sql_tax)
        connection.commit()
        print("[√] 供应商相关表结构初始化完成。")
    except Exception as e:
        connection.rollback()
        print(f"[x] 初始化数据库失败: {e}")
        raise e
    finally:
        cursor.close()

# ==============================================================================
# 3. API 请求逻辑 (签名与网络请求)
# ==============================================================================

def fetch_data_from_api(page_no):
    """
    调用开放平台接口拉取供应商数据。
    """
    current_milli = int(time.time() * 1000) 

    # [修改点 2] 动态构造 request_data
    # 基础参数
    request_data = {
        "corporationId": API_CONFIG['corporation_id'], 
        "pageNo": page_no,                             
        "pageSize": 50                                 
    }
    
    # 动态判断：只有当 org_code 有值时才加入请求参数
    if API_CONFIG.get('org_code'):
        request_data["orgCode"] = API_CONFIG['org_code']

    # 2. 生成签名 (Signature)
    body_str = json.dumps(request_data, separators=(',', ':'), ensure_ascii=False)
    timestamp_str = str(current_milli)
    sign_str = f"{API_CONFIG['app_key']}{API_CONFIG['secret_key']}{timestamp_str}{body_str}"
    signature = hashlib.md5(sign_str.encode('utf-8')).hexdigest()

    # 3. 构造 HTTP 请求头
    headers = {
        "Content-Type": "application/json",
        "Corporation-Id": str(API_CONFIG['corporation_id']),
        "RS-OpenAPI-Timestamp": timestamp_str,
        "RS-OpenAPI-TraceId": str(uuid.uuid4()),      
        "RS-OpenAPI-GrantType": "signature",          
        "RS-OpenAPI-AppKey": API_CONFIG['app_key'],
        "RS-OpenAPI-Signature": signature             
    }

    url = f"https://{API_CONFIG['host']}{API_CONFIG['path']}"

    try:
        resp = requests.post(url, headers=headers, data=body_str.encode('utf-8'), timeout=15)

        if resp.status_code != 200:
            print(f"[x] API请求失败 HTTP {resp.status_code}: {resp.text}")
            return None

        resp_json = resp.json()

        if resp_json.get("openapi-code") != "0":
            error_msg = resp_json.get('openapi-msg', 'Unknown Error')
            print(f"[x] 业务报错: {error_msg}")
            # 注意：如果API服务端强制要求 orgCode，这里会报错，提示参数缺失
            return None

        biz_data = resp_json.get("biz-data", {})
        supplier_list = biz_data.get("supplierList", [])
        return supplier_list

    except Exception as e:
        print(f"[!] API网络请求发生异常: {e}")
        return None

# ==============================================================================
# 4. 数据保存逻辑 (ETL)
# ==============================================================================

def save_to_db(connection, data_list):
    """
    解析 API 返回的 JSON 数据并写入 MySQL。
    """
    if not data_list:
        return

    print(f"[-] 正在写入 {len(data_list)} 条供应商数据...")
    cursor = connection.cursor()
    inserted_count = 0

    sql_insert_main = """
        REPLACE INTO `supplier_main` (
            id, supplier_code, supplier_name, short_name, is_enabled,
            category_code, category_name,
            contact_name, contact_phone, email, region_name, 
            province_name, city_name, district_name, address_detail,
            tax_entity_name, tax_identification_no
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    sql_insert_account = """
        INSERT INTO `supplier_account` (
            supplier_id, account_name, account_no, channel_name, is_default
        ) VALUES (%s, %s, %s, %s, %s)
    """

    sql_insert_tax = """
        INSERT INTO `supplier_tax` (
            supplier_id, tax_name, tax_code, tax_type, tax_value
        ) VALUES (%s, %s, %s, %s, %s)
    """

    try:
        for item in data_list:
            supplier_id = item.get('id')
            if not supplier_id: 
                continue 

            contact = item.get('contactInfo') or {}
            
            addr_parts = [
                contact.get('street1'), 
                contact.get('street2'), 
                contact.get('houseNumber')
            ]
            full_address = " ".join([str(x) for x in addr_parts if x])
            
            finance = item.get('financeExtInfo') or {}

            try:
                cursor.execute(sql_insert_main, (
                    supplier_id,
                    item.get('supplierCode'),
                    item.get('supplierName'),
                    item.get('shortName'),
                    1 if item.get('isEnabled') is True else 0,
                    item.get('categoryCode'),
                    item.get('categoryName'),
                    contact.get('contactBy'),
                    contact.get('contactNumber'),
                    contact.get('email'),
                    contact.get('regionName'),
                    contact.get('provinceName'),
                    contact.get('cityName'),
                    contact.get('districtName'),
                    full_address,
                    finance.get('taxEntityName'),
                    finance.get('taxIdentificationNo')
                ))
            except Exception as e:
                print(f"[!] 主表写入失败(ID={supplier_id}): {e}")
                continue 

            account_list = item.get('accountInfoList', [])
            if account_list:
                cursor.execute("DELETE FROM supplier_account WHERE supplier_id = %s", (supplier_id,))
                for acc in account_list:
                    cursor.execute(sql_insert_account, (
                        supplier_id,
                        acc.get('accountName'),
                        acc.get('accountNo'),
                        acc.get('channelName'),
                        acc.get('isDefault')
                    ))

            tax_list = item.get('taxInfoList', [])
            if tax_list:
                cursor.execute("DELETE FROM supplier_tax WHERE supplier_id = %s", (supplier_id,))
                for tax in tax_list:
                    cursor.execute(sql_insert_tax, (
                        supplier_id,
                        tax.get('taxName'),
                        tax.get('taxCode'),
                        tax.get('taxType'),
                        tax.get('taxValue')
                    ))

            inserted_count += 1

        connection.commit()
        print(f"[√] 成功处理 {inserted_count} 个供应商。")

    except Exception as e:
        connection.rollback()
        print(f"[x] 数据库写入过程中发生严重错误: {e}")
    finally:
        cursor.close()

# ==============================================================================
# 5. 主程序入口
# ==============================================================================

def main():
    print(f"=== Resto 供应商数据同步任务 [{time.strftime('%Y-%m-%d %H:%M:%S')}] ===")

    # [修改点 3] 移除了对 org_code 的强制检查，或者您可以保留检查但允许为 None
    # 之前的检查代码已被移除，以免阻断运行

    conn = None
    try:
        conn = pymysql.connect(**DB_CONFIG)
        init_database(conn)

        page = 1
        total_fetched = 0

        while True:
            print(f"[-] 正在查询第 {page} 页...")

            supplier_list = fetch_data_from_api(page)

            if not supplier_list:
                print("[-] 接口未返回数据，抓取结束。")
                break

            save_to_db(conn, supplier_list)
            total_fetched += len(supplier_list)

            if len(supplier_list) < 50:
                print("[-] 数据量小于页大小，已达到最后一页。")
                break

            page += 1
            time.sleep(0.5) 

        print(f"[√] 同步任务结束，共更新/插入 {total_fetched} 条供应商数据。")

    except Exception as e:
        print(f"[!] 脚本主流程发生异常: {e}")
    finally:
        if conn:
            conn.close()
            print("[-] 数据库连接已关闭。")

    print("=== End ===")

if __name__ == '__main__':
    main()