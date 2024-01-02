# !/usr/bin/python
# -*- coding: utf-8 -*-
"""
@File    :  index.py
@Time    :  2023/10/19 17:12
@Author  :  Kyrie
@Email   :  Kyrie.Lu@littlefreddie.com
@Version :  1.0
@License :  (C)Copyright 2021-2022
@Desc    :  云听数据ods-dwd层处理
"""
import ast
import json
import warnings

import odps
import pandas as pd
from sqlalchemy import create_engine

from setting import config

warnings.filterwarnings("ignore")


class YtReviewInc:
    def __init__(self, access_id, secret_access_key, project, endpoint):
        # odps对象
        self.odps = odps.ODPS(access_id=access_id, secret_access_key=secret_access_key,
                              project=project, endpoint=endpoint)
        self.engine = create_engine(url=config.yt_data_database_uri)
        self.main_table = "dwd_yt_ec_review_main_test"
        self.aspect_table = "dwd_yt_ec_review_aspect_test"
        self.source_table_name = "yt_ec_review_test"

    def work(self):
        df = self.get_data_by_rds()
        if df.empty:
            print(f"没有新增/变更数据。。。")
            return
        # 重命名
        df.rename(columns={"unique_id": "uniques_id"}, inplace=True)
        # 提取配置信息中的品牌
        df["configs"] = df["configs"].apply(YtReviewInc.dispose_configs)
        df[["brand", "store", "platform", "category", "goods_id", "top_level", "platform_level"]] = \
            df["configs"].str.split("=", expand=True)
        aspect_df = df[["id", "aspect", "uniques_id", "create_time", "modify_time", "send_date"]]
        df.drop(columns=["aspect", "configs", "insert_timestamp", "send_batch"], inplace=True)
        # 删除MaxCompute已存在的记录
        self.delete_data(self.main_table, df["uniques_id"].tolist())
        # 主表数据入库
        self.save_main_by_odps(df)
        # 筛选值不为空的数据
        aspect_df = aspect_df[aspect_df["aspect"] != '[]']
        if not aspect_df.empty:
            # 将str转list
            aspect_df["aspect"] = aspect_df["aspect"].map(lambda x: json.loads(x))
            # 一列拆成多行
            aspect_df = aspect_df.explode("aspect")
            aspect_df["aspect"] = aspect_df["aspect"].apply(YtReviewInc.dispose_aspect)
            aspect_df[["escore", "aspect1", "aspect2"]] = aspect_df["aspect"].str.split("-", expand=True)
            aspect_df.drop(columns=["aspect"], inplace=True)
            # 删除MaxCompute已存在的记录
            self.delete_data(self.aspect_table, aspect_df["uniques_id"].tolist())
            # 指标数据入库
            self.save_aspect_by_odps(aspect_df)

    @staticmethod
    def dispose_aspect(json_data):
        """提取json数据中信息"""
        escore = json_data.get("escore")
        aspect1 = json_data.get("aspect1")
        aspect2 = json_data.get("aspect2")
        return escore + "-" + aspect1 + "-" + aspect2

    @staticmethod
    def dispose_configs(json_data):
        """提取json数据中信息"""
        li = ast.literal_eval(json_data)
        config_dict = {}
        for cate in li:
            config_dict.update(cate)
        brand = config_dict.get("品牌")
        store = config_dict.get("店铺")
        platform = config_dict.get("电商平台")
        category = config_dict.get("品类")
        goods_id = config_dict.get("商品ID")
        top_level = config_dict.get("大类")
        platform_level = config_dict.get("电商平台分类")
        values = brand + "=" + store + "=" + platform + "=" + category + "=" + goods_id + "=" + top_level + "=" + \
                 platform_level
        return values

    def get_data_by_rds(self):
        """获取rds数据库中原始表数据"""
        sql = f"SELECT * FROM {self.source_table_name} LIMIT 500"
        yt_df = pd.read_sql(sql, self.engine)
        return yt_df

    def delete_data(self, table_name, uniques_id_list):
        """根据uniques_id删除MaxCompute中存在的记录"""
        sql = f"DELETE FROM {table_name} WHERE uniques_id in {tuple(uniques_id_list)};"
        result = self.odps.execute_sql(sql)
        if not result:
            raise "删除主表数据异常"

    def save_main_by_odps(self, temp_df):
        """保存主表数据到MaxCompute"""
        temp_df = temp_df[["id", "analyzer", "c_time", "brand", "category", "goods_id", "top_level", "store",
                           "platform", "platform_level", "connection_name", "content", "data_level", "escore",
                           "group_id", "oid", "parent", "project_name", "score", "source_name", "sku", "detail",
                           "title", "u_name", "uniques_id", "url", "is_default", "pictures", "videos", "is_plus",
                           "multiple_group", "created_at", "tag_name", "send_date", "create_time", "modify_time"]]
        result = self.odps.write_table(self.main_table, temp_df.values.tolist())
        if not result:
            print(f"主表数据入库失败")
            raise result

    def save_aspect_by_odps(self, temp_df):
        """保存指标数据到MaxCompute"""
        temp_df = temp_df[["id", "uniques_id", "escore", "aspect1", "aspect2", "send_date", "create_time",
                           "modify_time"]]
        result = self.odps.write_table(self.aspect_table, temp_df.values.tolist())
        if not result:
            print(f"指标数据入库失败")
            raise result


def handler(event, context):
    evt = ast.literal_eval(event.decode())
    access_id_ = evt.get("access_id")
    secret_access_key_ = evt.get("secret_access_key")
    project_ = evt.get("project")
    endpoint_ = evt.get("endpoint")
    YtReviewInc(access_id_, secret_access_key_, project_, endpoint_).work()
    return 'ok'
