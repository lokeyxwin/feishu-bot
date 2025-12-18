import os
import json
import requests
import sys
import urllib.parse
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime

# === input params start
app_id = os.getenv("APP_ID")        # app_id, required, 应用 ID
# 应用唯一标识，创建应用后获得。有关app_id 的详细介绍。请参考通用参数https://open.feishu.cn/document/ukTMukTMukTM/uYTM5UjL2ETO14iNxkTN/terminology。
app_secret = os.getenv("APP_SECRET")  # app_secret, required, 应用密钥
# 应用秘钥，创建应用后获得。有关 app_secret 的详细介绍，请参考https://open.feishu.cn/document/ukTMukTMukTM/uYTM5UjL2ETO14iNxkTN/terminology。
base_url = os.getenv("BASE_URL")    # string, required, 多维表格URL
# 多维表格的完整URL，用于解析app_token、table_id等参数。格式如：https://lcn77os9cl0o.feishu.cn/base/PzITbIyJfaB03BsqVtIcrjtznFf?from=from_copylink
# === input params end

def get_tenant_access_token(app_id: str, app_secret: str) -> Tuple[str, Exception]:
    """获取 tenant_access_token

    Args:
        app_id: 应用ID
        app_secret: 应用密钥

    Returns:
        Tuple[str, Exception]: (access_token, error)
    """
    url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
    payload = {
        "app_id": app_id,
        "app_secret": app_secret
    }
    headers = {
        "Content-Type": "application/json; charset=utf-8"
    }
    try:
        print(f"POST: {url}")
        print(f"\nRequest payload: {json.dumps(payload)}\n")
        response = requests.post(url, json=payload, headers=headers)
        response.raise_for_status()

        result = response.json()
        print(f"Response: {json.dumps(result)}\n")

        if result.get("code", 0) != 0:
            print(f"Error: failed to get tenant_access_token: {result.get('msg', 'unknown error')}", file=sys.stderr)
            return "", Exception(f"failed to get tenant_access_token: {response.text}")

        return result["tenant_access_token"], None

    except Exception as e:
        error_msg = str(e)
        if hasattr(e, 'response') and e.response is not None:
            error_msg += " " + e.response.text
        print(f"Error: getting tenant_access_token: {error_msg}", file=sys.stderr)
        return "", e

def get_wiki_node_info(tenant_access_token: str, node_token: str) -> Dict[str, Any]:
    """获取知识空间节点信息

    Args:
        tenant_access_token: 租户访问令牌
        node_token: 节点令牌

    Returns:
        Dict[str, Any]: 节点信息对象
    """
    url = f"https://open.feishu.cn/open-apis/wiki/v2/spaces/get_node?token={urllib.parse.quote(node_token)}"
    headers = {
        "Authorization": f"Bearer {tenant_access_token}",
        "Content-Type": "application/json; charset=utf-8"
    }

    try:
        print(f"GET: {url}")
        response = requests.get(url, headers=headers)
        response.raise_for_status()

        result = response.json()
        if result.get("code", 0) != 0:
            print(f"ERROR: 获取知识空间节点信息失败 {result}", file=sys.stderr)
            raise Exception(f"failed to get wiki node info: {result.get('msg', 'unknown error')}")

        if not result.get("data") or not result["data"].get("node"):
            raise Exception("未获取到节点信息")

        node_info = result["data"]["node"]
        print("节点信息获取成功:", {
            "node_token": node_info.get("node_token"),
            "obj_type": node_info.get("obj_type"),
            "obj_token": node_info.get("obj_token"),
            "title": node_info.get("title")
        })
        return node_info

    except Exception as e:
        print(f"Error getting wiki node info: {e}", file=sys.stderr)
        raise

def parse_base_url(tenant_access_token: str, base_url_string: str) -> Dict[str, Optional[str]]:
    """解析多维表格参数

    Args:
        tenant_access_token: 租户访问令牌
        base_url_string: 基础URL字符串

    Returns:
        Dict[str, Optional[str]]: 包含appToken、tableID、viewID的字典
    """
    from urllib.parse import urlparse, parse_qs

    parsed_url = urlparse(base_url_string)
    pathname = parsed_url.path
    app_token = pathname.split("/")[-1]

    # 如果URL包含wiki路径，需要获取节点信息
    if "/wiki/" in pathname:
        node_info = get_wiki_node_info(tenant_access_token, app_token)
        app_token = node_info.get("obj_token", app_token)

    query_params = parse_qs(parsed_url.query)
    view_id = query_params.get("view", [None])[0]
    table_id = query_params.get("table", [None])[0]

    # 如果没有从URL中获取到table_id，需要通过API获取所有数据表并匹配表名
    if table_id is None:
        print("未从URL中获取到table_id，将通过API获取所有数据表...")
        tables = list_bitable_tables(tenant_access_token, app_token)
        # 查找名为"⏰客户管理表"的数据表
        for table in tables:
            if table.get("name") == "⏰客户管理表":
                table_id = table.get("table_id")
                print(f"找到匹配的表名'⏰客户管理表'，table_id为: {table_id}")
                break
        
        if table_id is None:
            # 如果还是没有找到，抛出错误
            error_msg = "无法从BASE_URL解析出table_id，且未能通过API找到名为'⏰客户管理表'的数据表"
            print(f"ERROR: {error_msg}", file=sys.stderr)
            raise Exception(error_msg)

    return {
        "app_token": app_token,
        "table_id": table_id,
        "view_id": view_id
    }

def list_bitable_tables(tenant_access_token: str, app_token: str) -> List[Dict[str, Any]]:
    """列出多维表格中的所有数据表

    Args:
        tenant_access_token: 租户访问令牌
        app_token: 多维表格App的唯一标识

    Returns:
        List[Dict[str, Any]]: 数据表列表
    """
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables"
    headers = {
        "Authorization": f"Bearer {tenant_access_token}",
        "Content-Type": "application/json; charset=utf-8"
    }

    try:
        print(f"GET: {url}")
        response = requests.get(url, headers=headers)
        response.raise_for_status()

        result = response.json()
        if result.get("code", 0) != 0:
            print(f"ERROR: 列出数据表失败 {result}", file=sys.stderr)
            raise Exception(f"failed to list bitable tables: {result.get('msg', 'unknown error')}")

        if not result.get("data") or not result["data"].get("items"):
            return []

        tables = result["data"]["items"]
        print(f"成功获取到 {len(tables)} 个数据表")
        for table in tables:
            print(f"表名: {table.get('name')}, table_id: {table.get('table_id')}")
        
        return tables

    except Exception as e:
        print(f"Error listing bitable tables: {e}", file=sys.stderr)
        raise

if __name__ == "__main__":
    # 获取 tenant_access_token
    tenant_access_token, err = get_tenant_access_token(app_id, app_secret)
    if err:
        print(f"Error: getting tenant_access_token: {err}", file=sys.stderr)
        exit(1)
    
    # 解析多维表格参数
    try:
        bitable_params = parse_base_url(tenant_access_token, base_url)
        print(f"成功解析多维表格参数: {bitable_params}")
    except Exception as e:
        print(f"ERROR: 解析多维表格参数失败: {e}", file=sys.stderr)
        exit(1)