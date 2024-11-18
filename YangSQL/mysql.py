#!/usr/bin/env python
# -*- coding: UTF-8 -*-
"""
* Author: Zongjian Yang
* Date: 2024/10/22 下午5:19 
* Project: YangSQL 
* File: mysql.py
* IDE: PyCharm 
* Function:
"""
import os
import json
from typing import Union, List
import pandas as pd


class YangSQL:
    def __init__(self):
        self.data_dict_path = 'data_dictionary.json'
        self._initialize_data_dict()

    def _initialize_data_dict(self):
        """初始化数据字典文件，如果不存在则创建"""
        try:
            with open(self.data_dict_path, 'r') as f:
                self.data_dict = json.load(f)
        except FileNotFoundError:
            self.data_dict = {"tables": []}
            with open(self.data_dict_path, 'w') as f:
                json.dump(self.data_dict, f, indent=4)

    def create(self, table_name: str, primary_key: str = None, columns: dict = None) -> None:
        """
        创建表并保存为 CSV，同时更新数据字典。
        :param table_name: 表名
        :param primary_key: 主键属性
        :param columns: 列名及其约束（类型、是否为空等）
        """
        # 检查表是否已存在
        if any(table['table_name'] == table_name for table in self.data_dict['tables']):
            raise ValueError(f"Table {table_name} already exists.")

        # 提取列名和类型
        column_names = list(columns.keys())
        column_defs = []

        for col in columns:
            data_type = columns[col][0]  # 类型
            constraints = columns[col][1] if len(columns[col]) > 1 else []  # 约束，默认为空列表
            column_defs.append({"column_name": col, "data_type": data_type, "constraints": constraints})

        # 创建空表并保存为 CSV
        df = pd.DataFrame(columns=column_names)
        csv_file_path = f"{table_name}.csv"
        df.to_csv(csv_file_path, index=False)

        # 更新数据字典
        new_table_metadata = {
            "table_name": table_name,
            "file_path": csv_file_path,
            "columns": column_defs,
            "primary_key": primary_key
        }

        self.data_dict['tables'].append(new_table_metadata)
        with open(self.data_dict_path, 'w') as f:
            json.dump(self.data_dict, f, indent=4)

        print(f"Table {table_name} created successfully.")

    def alter(self, table_name: str, action: str, column_name: str = None, data_type: str = None) -> None:
        """
        修改表结构。
        :param table_name: 表名
        :param action: 操作类型（'ADD', 'DROP', 'MODIFY'）
        :param column_name: 列名
        :param data_type: 数据类型（当 ADD 或 MODIFY 时需要指定）
        """
        # 获取表的元数据
        table_metadata = next((table for table in self.data_dict['tables'] if table['table_name'] == table_name), None)
        if not table_metadata:
            raise ValueError(f"Table {table_name} does not exist.")
        df = pd.read_csv(table_metadata['file_path'])

        if action == 'ADD':
            # 添加新列
            if column_name in df.columns:
                raise ValueError(f"Column {column_name} already exists in table {table_name}.")
            df[column_name] = None  # 新列数据默认为 None
            table_metadata['columns'].append({"column_name": column_name, "data_type": data_type})
            print(f"Column {column_name} added to table {table_name}.")

        elif action == 'DROP':
            # 删除列
            if column_name not in df.columns:
                raise ValueError(f"Column {column_name} does not exist in table {table_name}.")
            df = df.drop(columns=[column_name])  # 从 DataFrame 删除列
            table_metadata['columns'] = [col for col in table_metadata['columns'] if col['column_name'] != column_name]
            print(f"Column {column_name} dropped from table {table_name}.")

        elif action == 'MODIFY':
            # 修改列数据类型
            if column_name not in df.columns:
                raise ValueError(f"Column {column_name} does not exist in table {table_name}.")
            for col in table_metadata['columns']:
                if col['column_name'] == column_name:
                    col['data_type'] = data_type
                    print(f"Column {column_name} modified in table {table_name}.")
                    break

        else:
            raise ValueError(f"Unsupported action: {action}. Use 'ADD', 'DROP', or 'MODIFY'.")

        df.to_csv(table_metadata['file_path'], index=False)

        # 更新数据字典
        with open(self.data_dict_path, 'w') as f:
            json.dump(self.data_dict, f, indent=4)

    def drop(self, table_name: str) -> None:
        """
        删除指定表及其对应的 CSV 文件，并从数据字典中删除该表的信息。
        :param table_name: 要删除的表名
        """
        table_metadata = next((table for table in self.data_dict['tables'] if table['table_name'] == table_name), None)
        if not table_metadata:
            raise ValueError(f"Table {table_name} does not exist.")

        # 删除表对应的 CSV 文件
        csv_file_path = table_metadata['file_path']
        if os.path.exists(csv_file_path):
            os.remove(csv_file_path)

        # 从数据字典中删除该表
        self.data_dict['tables'] = [table for table in self.data_dict['tables'] if table['table_name'] != table_name]

        # 更新数据字典文件
        with open(self.data_dict_path, 'w') as f:
            json.dump(self.data_dict, f, indent=4)

        print(f"Table {table_name} dropped successfully.")

    def insert(self, table_name: str, values: List[dict]) -> None:
        """
        向指定表插入一个或多个元组，支持主键检查、数据类型检查和约束检查。
        :param table_name: 表名
        :param values: 要插入的数据列表，每个元素是一个字典，键为列名，值为对应的插入值
        """
        # 查找表的元数据
        table_metadata = next((table for table in self.data_dict['tables'] if table['table_name'] == table_name), None)
        if not table_metadata:
            raise ValueError(f"Table {table_name} does not exist.")

        table_columns = {col['column_name']: col for col in table_metadata['columns']}
        primary_key = table_metadata.get("primary_key")
        df = pd.read_csv(table_metadata['file_path'])

        for record in values:
            # 检查列数量是否匹配
            if len(record) > len(table_columns):
                raise ValueError(f"Record has more columns than defined in the table {table_name}.")
            if set(record.keys()) - set(table_columns):
                raise ValueError("Values contain columns not defined in the table schema.")

            # 检查主键是否存在
            pk_value = record.get(primary_key)
            if pk_value is None:
                raise ValueError(f"Primary key {primary_key} must be provided in values.")

            # 检查列是否存在、数据类型和约束
            for col, value in record.items():
                if col not in table_columns:
                    raise ValueError(f"Column {col} does not exist in table {table_name}.")

                column_meta = table_columns[col]
                data_type = column_meta['data_type']
                constraints = column_meta['constraints']

                # 检查数据类型
                if not self._check_data_type(value, data_type):
                    raise ValueError(
                        f"Column {col} in table {table_name} requires {data_type} but got {type(value).__name__}.")

                # 检查 NOT NULL 约束
                if 'NOT NULL' in constraints and value is None:
                    raise ValueError(f"Column {col} cannot be NULL.")

            # 检查主键是否存在，若存在则覆盖
            if pk_value in df[primary_key]:
                df.loc[df[primary_key] == pk_value, record.keys()] = list(record.values())
            else:
                new_row = pd.DataFrame([{col: record.get(col, None) for col in table_columns}])
                df = pd.concat([df, new_row], ignore_index=True)

        # 保存更新后的 DataFrame
        df.to_csv(table_metadata['file_path'], index=False)
        print(f"Inserted into {table_name}: {len(values)} records")

    @staticmethod
    def _check_data_type(value, data_type: str) -> bool:
        """
        检查值是否符合数据类型。
        :param value: 要检查的值
        :param data_type: 数据类型字符串
        :return: True 表示类型符合，False 表示不符合
        """
        if data_type.startswith("CHAR"):
            # 获取 CHAR 类型的最大长度
            max_len = int(data_type.split("(")[1].split(")")[0])
            return isinstance(value, str) and len(value) <= max_len
        elif data_type == "FLOAT":
            return isinstance(value, (float, int))  # int 也可以视为符合
        elif data_type == "INT":
            return isinstance(value, int)
        return False

    def update(self, table_name: str, set_values: dict[str, Union[str, int, float]], where: str = None) -> None:
        """
        更新表中某元组的数据，支持原地更新。
        :param table_name: 表名
        :param set_values: 要更新的数据，可以是新的值或更新表达式
        :param where: 可选的过滤条件
        """
        # 获取表的元数据
        table_metadata = next((table for table in self.data_dict['tables'] if table['table_name'] == table_name), None)
        if not table_metadata:
            raise ValueError(f"Table {table_name} does not exist.")

        df = pd.read_csv(table_metadata['file_path'])

        # 如果存在 WHERE 条件
        if where:
            # 创建一个布尔掩码，标记满足条件的行
            mask = df.query(where).index
            if mask.empty:
                print("No rows match the WHERE condition.")
                return
        else:
            # 如果没有 WHERE 条件，则选择所有行
            mask = df.index

        # 更新符合条件的行
        for col, new_value in set_values.items():
            if col not in df.columns:
                raise ValueError(f"Column {col} does not exist in table {table_name}.")
            if callable(new_value):
                df.loc[mask, col] = df.loc[mask, col].apply(new_value)
            else:
                df.loc[mask, col] = new_value  # 更新为新值

        # 保存完整的 DataFrame 到 .csv 文件
        df.to_csv(table_metadata['file_path'], index=False)
        print(f"Updated {table_name} with values: {set_values} where {where if where else 'no condition'}")

    def delete(self, params: dict) -> None:
        """
        删除表中的特定元组。
        :param params: 包含DELETE方法的参数(表名, 条件)
        """
        table_name = params.get('table_name')
        where = params.get('condition', None)

        # 获取表的元数据
        table_metadata = next((table for table in self.data_dict['tables'] if table['table_name'] == table_name), None)
        if not table_metadata:
            raise ValueError(f"Table {table_name} does not exist.")

        df = pd.read_csv(table_metadata['file_path'])

        if where is not None:
            # 筛选出符合条件的行
            matching_rows = df.query(where)
            if matching_rows.empty:
                print("No rows match the WHERE condition.")
                return

            # 删除符合条件的行
            df = df.drop(matching_rows.index)
        else:
            # 如果没有提供条件，删除所有行
            df = pd.DataFrame(columns=df.columns)
            print(f"Deleted all rows from {table_name}.")

        # 保存更改
        df.to_csv(table_metadata['file_path'], index=False)
        print(f"Updated {table_name} after deletion.")

    def select(self, params: dict) -> pd.DataFrame:
        """
        处理 SELECT 查询的入口函数，支持基本查询、过滤、排序、分组和连接。
        :param params: 包含查询参数的字典
        :return: 查询结果的 DataFrame
        """
        tables = params.get('tables')
        columns = params.get('columns', '*')
        where = params.get('where', None)
        joins = params.get('joins', None)

        if isinstance(tables, str):
            tables = [tables]

        # 首先加载第一个表
        df = self._load_table(tables[0])

        # 处理多表 JOIN 查询
        if joins:
            for join in joins:
                join_table_name = join.get("join_table")
                join_condition = join.get("join_condition")
                join_table = self._load_table(join_table_name)

                # 解析 join_condition，并获取左右表前缀的列名
                left_on, right_on = [col.strip() for col in join_condition.split('=')]
                left_on = left_on.split('.')[-1]  # 获取左表列名
                right_on = right_on.split('.')[-1]  # 获取右表列名
                print(df.columns)

                # 使用不带表名前缀的列名进行连接
                df = pd.merge(df, join_table, left_on=left_on, right_on=right_on)

        # 合并多个表（如果 tables 列表中有多个表名）
        for table_name in tables[1:]:
            additional_df = self._load_table(table_name)
            df = pd.concat([df, additional_df], ignore_index=True)

        # 应用 WHERE 过滤条件
        if where:
            df = df.query(where)
        # 选择查询的列
        if len(columns) == 1 and columns[0] == '*':
            select_df = df
        else:
            select_df = df[columns]

        return select_df

    def _load_table(self, table_name: str) -> pd.DataFrame:
        """加载表格数据"""
        table_metadata = next((table for table in self.data_dict['tables'] if table['table_name'] == table_name), None)
        if not table_metadata:
            raise ValueError(f"Table {table_name} does not exist.")
        return pd.read_csv(table_metadata['file_path'])


if __name__ == '__main__':
    # 使用示例
    sql = YangSQL()
    parsed_values = [
        {'ssn': '230101198009081234', 'name': '张三', 'bdate': '1980-09-08', 'address': '哈尔滨道里区十二道街',
         'sex': '男', 'salary': 3125, 'superssn': '23010119751201312X', 'dno': 'd1'},
        {'ssn': '230101198107023736', 'name': '李四', 'bdate': '1981-07-02', 'address': '哈尔滨道外区三道街',
         'sex': '男', 'salary': 2980, 'superssn': '23010119751201312X', 'dno': 'd1'},
        {'ssn': '230101198009081234', 'name': '张三', 'bdate': '1980-09-08', 'address': '哈尔滨道里区十二道街',
         'sex': '男', 'salary': 3125, 'superssn': '23010119751201312X', 'dno': 'd1'}
        # 更多数据...
    ]

    # 调用 insert 方法
    # sql.insert('employee', parsed_values)
