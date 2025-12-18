#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
飞书机器人自动录入客户信息到指定多维表格解决方案
基于 Lark Python SDK 实现
"""

import os
import json
import logging
import asyncio
import redis
from datetime import datetime
from typing import Dict, Any, Optional, List
from urllib.parse import urlparse, parse_qs

import lark_oapi as lark
from lark_oapi.api.im.v1 import *
from lark_oapi.api.bitable.v1 import *
from lark_oapi.api.drive.v1 import *
from lark_oapi.api.contact.v3 import *
from lark_oapi.api.authen.v1 import *
from lark_oapi.event import BaseEvent
from lark_oapi.webhook.dispatcher import BaseDispatcher, MemEventDispatcher
from lark_oapi.webhook.event import BaseEvent
from lark_oapi.webhook.model import EventHeader
from lark_oapi.webhook.handler import EventHandler

from flask import Flask, request, jsonify
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# 配置参数
APP_ID = os.getenv("APP_ID")
APP_SECRET = os.getenv("APP_SECRET")
BASE_URL = os.getenv("BASE_URL")
TARGET_TABLE_NAME = os.getenv("TARGET_TABLE_NAME", "⏰客户管理表")
VERIFICATION_TOKEN = os.getenv("VERIFICATION_TOKEN", "")
ENCRYPT_KEY = os.getenv("ENCRYPT_KEY", "")

# Redis配置（用于状态管理）
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_DB = int(os.getenv("REDIS_DB", 0))

# 初始化Redis客户端
try:
    redis_client = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        db=REDIS_DB,
        decode_responses=True
    )
    redis_client.ping()
    logger.info("Redis连接成功")
except Exception as e:
    logger.warning(f"Redis连接失败: {e}，将使用内存缓存")
    redis_client = None

# 创建Flask应用
app = Flask(__name__)

# === 步骤一：构建 API Client ===
def create_lark_client():
    """创建Lark API客户端"""
    return lark.Client.builder() \
        .app_id(APP_ID) \
        .app_secret(APP_SECRET) \
        .domain(lark.FEISHU_DOMAIN) \
        .timeout(30) \
        .log_level(lark.LogLevel.INFO) \
        .build()

# 全局客户端
lark_client = create_lark_client()

# === 工具函数 ===
def get_user_state(user_id: str) -> Optional[Dict]:
    """获取用户状态"""
    if redis_client:
        state_json = redis_client.get(f"user_state:{user_id}")
        return json.loads(state_json) if state_json else None
    return None

def set_user_state(user_id: str, state: Dict, ttl: int = 300):
    """设置用户状态"""
    if redis_client:
        redis_client.setex(
            f"user_state:{user_id}",
            ttl,
            json.dumps(state, ensure_ascii=False)
        )

def delete_user_state(user_id: str):
    """删除用户状态"""
    if redis_client:
        redis_client.delete(f"user_state:{user_id}")

def parse_base_url() -> Dict[str, str]:
    """解析BASE_URL获取app_token和table_id"""
    parsed_url = urlparse(BASE_URL)
    path_parts = parsed_url.path.strip('/').split('/')
    
    # 获取app_token
    app_token = path_parts[-1] if path_parts else ""
    
    # 解析查询参数
    query_params = parse_qs(parsed_url.query)
    table_id = query_params.get('table', [None])[0]
    view_id = query_params.get('view', [None])[0]
    
    return {
        "app_token": app_token,
        "table_id": table_id,
        "view_id": view_id
    }

def get_target_table_id() -> str:
    """获取目标表的table_id"""
    # 缓存table_id，避免频繁调用API
    cache_key = "target_table_id"
    
    if redis_client:
        cached_id = redis_client.get(cache_key)
        if cached_id:
            return cached_id
    
    try:
        parsed_params = parse_base_url()
        app_token = parsed_params["app_token"]
        
        # 调用SDK获取表格列表
        request: ListAppTableRequest = ListAppTableRequest.builder() \
            .app_token(app_token) \
            .build()
        
        response: ListAppTableResponse = lark_client.bitable.v1.app_table.list(request)
        
        if not response.success():
            logger.error(f"获取表格列表失败: {response.msg}")
            raise Exception(f"获取表格列表失败: {response.msg}")
        
        # 查找目标表
        tables = response.data.items
        for table in tables:
            if table.name == TARGET_TABLE_NAME:
                table_id = table.table_id
                
                # 缓存结果
                if redis_client:
                    redis_client.setex(cache_key, 3600, table_id)
                
                return table_id
        
        raise Exception(f"未找到目标表: {TARGET_TABLE_NAME}")
        
    except Exception as e:
        logger.error(f"获取目标表ID失败: {e}")
        raise

def get_table_fields(app_token: str, table_id: str) -> List[Dict]:
    """获取表格字段信息"""
    try:
        request: ListAppTableFieldRequest = ListAppTableFieldRequest.builder() \
            .app_token(app_token) \
            .table_id(table_id) \
            .build()
        
        response: ListAppTableFieldResponse = lark_client.bitable.v1.app_table_field.list(request)
        
        if not response.success():
            logger.error(f"获取字段列表失败: {response.msg}")
            return []
        
        fields = []
        for field in response.data.items:
            field_info = {
                "field_id": field.field_id,
                "field_name": field.field_name,
                "type": field.type,
                "property": json.loads(str(field.property)) if field.property else {}
            }
            fields.append(field_info)
        
        return fields
        
    except Exception as e:
        logger.error(f"获取表格字段失败: {e}")
        return []

def get_single_select_option_id(fields: List[Dict], field_name: str, option_text: str) -> Optional[str]:
    """获取单选字段的选项ID"""
    for field in fields:
        if field["field_name"] == field_name and field["type"] == 3:  # 单选类型
            options = field.get("property", {}).get("options", [])
            for option in options:
                if option.get("name") == option_text:
                    return option.get("id")
    return None

def check_duplicate_record(app_token: str, table_id: str, phone: str = None, wechat: str = None) -> tuple:
    """检查电话/微信是否重复"""
    try:
        conditions = []
        if phone:
            conditions.append({
                "field_name": "电话",
                "operator": "is",
                "value": [phone]
            })
        if wechat:
            conditions.append({
                "field_name": "微信",
                "operator": "is",
                "value": [wechat]
            })
        
        if not conditions:
            return False, None
        
        filter_condition = {
            "conjunction": "or",
            "conditions": conditions
        }
        
        request: SearchAppTableRecordRequest = SearchAppTableRecordRequest.builder() \
            .app_token(app_token) \
            .table_id(table_id) \
            .request_body(SearchAppTableRecordRequestBody.builder()
                .field_names(["客户ID"])
                .filter(json.dumps(filter_condition, ensure_ascii=False))
                .build()) \
            .build()
        
        response: SearchAppTableRecordResponse = lark_client.bitable.v1.app_table_record.search(request)
        
        if not response.success():
            logger.error(f"查询记录失败: {response.msg}")
            return False, None
        
        if response.data.items and len(response.data.items) > 0:
            record = response.data.items[0]
            return True, record.record_id
        
        return False, None
        
    except Exception as e:
        logger.error(f"查重失败: {e}")
        return False, None

def upload_image(image_key: str) -> Optional[str]:
    """上传图片获取file_token"""
    try:
        # 首先下载图片
        request: DownloadMediaRequest = DownloadMediaRequest.builder() \
            .file_token(image_key) \
            .build()
        
        response: DownloadMediaResponse = lark_client.drive.v1.media.download(request)
        
        if not response.success():
            logger.error(f"下载图片失败: {response.msg}")
            return None
        
        # 这里需要根据实际需求处理文件上传
        # 由于上传素材API需要具体的文件数据，这里简化处理
        # 实际使用时需要完善文件上传逻辑
        
        # 返回一个示例file_token，实际应该使用上传后的真实token
        return f"image_{image_key}"
        
    except Exception as e:
        logger.error(f"上传图片失败: {e}")
        return None

def create_customer_record(app_token: str, table_id: str, fields_data: Dict) -> bool:
    """创建客户记录"""
    try:
        # 获取表格字段信息
        table_fields = get_table_fields(app_token, table_id)
        
        # 准备fields数据
        fields = {}
        
        # 处理单选字段（渠道、来源）
        for field_name in ["渠道", "来源"]:
            if field_name in fields_data:
                option_text = fields_data[field_name]
                option_id = get_single_select_option_id(table_fields, field_name, option_text)
                if option_id:
                    fields[field_name] = option_id
                else:
                    fields[field_name] = option_text
        
        # 处理其他字段
        for field_name, value in fields_data.items():
            if field_name not in ["渠道", "来源"] and value:
                fields[field_name] = value
        
        # 处理日期字段
        if "录入日期" not in fields:
            fields["录入日期"] = datetime.now().strftime("%Y-%m-%d")
        
        # 创建记录请求
        request: CreateAppTableRecordRequest = CreateAppTableRecordRequest.builder() \
            .app_token(app_token) \
            .table_id(table_id) \
            .request_body(CreateAppTableRecordRequestBody.builder()
                .fields(json.dumps(fields, ensure_ascii=False))
                .build()) \
            .build()
        
        response: CreateAppTableRecordResponse = lark_client.bitable.v1.app_table_record.create(request)
        
        if not response.success():
            logger.error(f"创建记录失败: {response.msg}")
            return False
        
        logger.info(f"记录创建成功: {response.data.record.record_id}")
        return True
        
    except Exception as e:
        logger.error(f"创建客户记录失败: {e}")
        return False

# === 消息处理逻辑 ===
def parse_customer_info(text: str) -> Dict[str, str]:
    """解析客户信息"""
    import re
    
    patterns = {
        "渠道": r"渠道[:：]\s*(.+)",
        "来源": r"来源[:：]\s*(.+)", 
        "电话": r"电话[:：]\s*(.+)",
        "微信": r"微信[:：]\s*(.+)"
    }
    
    result = {}
    for field, pattern in patterns.items():
        match = re.search(pattern, text)
        if match:
            result[field] = match.group(1).strip()
    
    return result

async def send_message(chat_id: str, content: str, msg_type: str = "text"):
    """发送消息"""
    try:
        request: CreateMessageRequest = CreateMessageRequest.builder() \
            .receive_id_type("chat_id") \
            .request_body(CreateMessageRequestBody.builder()
                .receive_id(chat_id)
                .msg_type(msg_type)
                .content(json.dumps({"text": content}))
                .build()) \
            .build()
        
        response: CreateMessageResponse = await lark_client.im.v1.message.acreate(request)
        
        if not response.success():
            logger.error(f"发送消息失败: {response.msg}")
            return False
        
        return True
        
    except Exception as e:
        logger.error(f"发送消息异常: {e}")
        return False

# === Webhook处理 ===
@app.route('/webhook', methods=['POST'])
def webhook():
    """处理飞书Webhook事件"""
    try:
        # 获取请求数据
        data = request.get_json()
        logger.info(f"收到Webhook事件: {json.dumps(data, indent=2, ensure_ascii=False)}")
        
        # 验证签名
        if VERIFICATION_TOKEN:
            token = request.headers.get('X-Lark-Verification-Token')
            if token != VERIFICATION_TOKEN:
                return jsonify({"error": "Invalid token"}), 403
        
        # 处理挑战请求
        if data.get("type") == "url_verification":
            challenge = data.get("challenge")
            return jsonify({"challenge": challenge})
        
        # 处理事件回调
        event = data.get("event", {})
        event_type = event.get("type")
        
        if event_type == "im.message.receive_v1":
            return handle_message_event(event)
        
        return jsonify({"msg": "Event received"}), 200
        
    except Exception as e:
        logger.error(f"处理Webhook异常: {e}")
        return jsonify({"error": str(e)}), 500

def handle_message_event(event: Dict) -> Dict:
    """处理消息事件"""
    try:
        message = event.get("message", {})
        sender = event.get("sender", {})
        
        message_id = message.get("message_id")
        chat_id = message.get("chat_id", {})
        chat_type = message.get("chat_type")
        content = json.loads(message.get("content", "{}"))
        text = content.get("text", "").strip()
        
        sender_id = sender.get("sender_id", {})
        user_id = sender_id.get("user_id") if sender_id else None
        
        logger.info(f"收到消息 - 用户: {user_id}, 群聊: {chat_id}, 内容: {text[:50]}")
        
        # 检查是否@了机器人
        if chat_type == "group" and "@_user_1" in text:
            # 发送模板提示
            template = """请按以下模板提供信息：
渠道：
来源：
电话：
微信：
（可直接发送图片，将自动作为初次聊天记录附件）"""
            
            # 异步发送消息
            asyncio.run(send_message(chat_id, template))
            
            # 设置用户状态
            set_user_state(user_id, {
                "chat_id": chat_id,
                "step": "waiting_info",
                "created_at": datetime.now().isoformat()
            })
            
        # 处理用户回复
        elif chat_type == "p2p":
            user_state = get_user_state(user_id)
            
            if user_state and user_state.get("step") == "waiting_info":
                # 解析客户信息
                customer_info = parse_customer_info(text)
                
                if customer_info:
                    # 验证电话或微信至少有一个
                    phone = customer_info.get("电话")
                    wechat = customer_info.get("微信")
                    
                    if not phone and not wechat:
                        asyncio.run(send_message(
                            user_state["chat_id"],
                            "电话和微信至少需要填写一个"
                        ))
                        return jsonify({"msg": "Validation failed"}), 200
                    
                    # 获取多维表格信息
                    parsed_params = parse_base_url()
                    app_token = parsed_params["app_token"]
                    table_id = get_target_table_id()
                    
                    # 检查重复
                    is_duplicate, duplicate_id = check_duplicate_record(
                        app_token, table_id, phone, wechat
                    )
                    
                    if is_duplicate:
                        asyncio.run(send_message(
                            user_state["chat_id"],
                            f"数据与客户ID：{duplicate_id}重复"
                        ))
                        delete_user_state(user_id)
                        return jsonify({"msg": "Duplicate found"}), 200
                    
                    # 创建记录
                    fields_data = {
                        "渠道": customer_info.get("渠道", ""),
                        "来源": customer_info.get("来源", ""),
                        "电话": phone,
                        "微信": wechat
                    }
                    
                    success = create_customer_record(app_token, table_id, fields_data)
                    
                    if success:
                        asyncio.run(send_message(
                            user_state["chat_id"],
                            "客户信息已成功录入！"
                        ))
                    else:
                        asyncio.run(send_message(
                            user_state["chat_id"],
                            "录入失败，请稍后重试"
                        ))
                    
                    # 清理状态
                    delete_user_state(user_id)
                    
                else:
                    # 格式错误，重新提示
                    asyncio.run(send_message(
                        user_state["chat_id"],
                        "格式不正确，请按模板提供信息"
                    ))
        
        return jsonify({"msg": "Message processed"}), 200
        
    except Exception as e:
        logger.error(f"处理消息事件异常: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/health', methods=['GET'])
def health_check():
    """健康检查"""
    return jsonify({
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "app_id": APP_ID,
        "redis": "connected" if redis_client else "disabled"
    })

@app.route('/config', methods=['GET'])
def show_config():
    """显示配置信息"""
    parsed_params = parse_base_url()
    return jsonify({
        "app_id": APP_ID,
        "base_url": BASE_URL,
        "target_table": TARGET_TABLE_NAME,
        "parsed_params": parsed_params,
        "table_id": get_target_table_id() if parsed_params.get("app_token") else None
    })

# === 主函数 ===
if __name__ == '__main__':
    # 验证配置
    if not all([APP_ID, APP_SECRET, BASE_URL]):
        logger.error("环境变量配置不完整")
        exit(1)
    
    # 显示配置信息
    logger.info("=== 飞书机器人配置 ===")
    logger.info(f"App ID: {APP_ID}")
    logger.info(f"Base URL: {BASE_URL}")
    logger.info(f"目标表: {TARGET_TABLE_NAME}")
    
    # 解析并验证表格
    try:
        table_id = get_target_table_id()
        logger.info(f"目标表ID: {table_id}")
    except Exception as e:
        logger.error(f"表格验证失败: {e}")
        exit(1)
    
    # 启动服务
    port = int(os.getenv("PORT", 5000))
    logger.info(f"启动服务在端口: {port}")
    
    app.run(
        host='0.0.0.0',
        port=port,
        debug=os.getenv("FLASK_DEBUG", "false").lower() == "true"
    )