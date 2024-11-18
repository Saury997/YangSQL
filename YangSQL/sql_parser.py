import re
import sqlparse


def check_sql_syntax(sql):
    try:
        parsed = sqlparse.parse(sql)
        if not parsed:
            return False, "Invalid SQL syntax"
        return True, "SQL syntax is correct"
    except Exception as e:
        return False, str(e)


def parse_create(sql_statement: str):
    # 去掉首尾空格
    sql_statement = sql_statement.strip()

    # 提取表名
    table_name_pattern = r"CREATE TABLE (\w+)"
    table_name_match = re.search(table_name_pattern, sql_statement, re.IGNORECASE)
    if table_name_match:
        table_name = table_name_match.group(1)
    else:
        raise ValueError("未找到表名")

    # 提取表中的列定义部分（括号内的内容）
    columns_pattern = r"\((.*)\)"
    columns_match = re.search(columns_pattern, sql_statement, re.DOTALL)
    if columns_match:
        columns_block = columns_match.group(1).strip()
    else:
        raise ValueError("未找到列定义部分")

    # 按逗号分割每个列定义
    columns_definitions = columns_block.split(',')
    columns = {}
    primary_key = None

    # 逐列解析每个列定义
    for col_def in columns_definitions:
        col_def = col_def.strip()

        # 分离列名、数据类型和约束条件
        parts = col_def.split()

        if len(parts) < 2:
            raise ValueError(f"无法解析列定义：{col_def}")

        column_name = parts[0]
        data_type = parts[1]
        constraints = []

        # 检查常见的约束条件
        if 'PRIMARY KEY' in col_def:
            primary_key = column_name
            constraints.append('PRIMARY KEY')

        if 'NOT NULL' in col_def:
            constraints.append('NOT NULL')

        if 'UNIQUE' in col_def:
            constraints.append('UNIQUE')

        # 处理简单的 CHECK 约束
        check_pattern = r"CHECK\s*\((.+)\)"
        check_match = re.search(check_pattern, col_def)
        if check_match:
            check_condition = check_match.group(1).strip()
            constraints.append(check_condition)  # 只提取布尔表达式，不包含 "CHECK"

        # 将列名和(数据类型, 约束)作为元组存入字典
        columns[column_name] = (data_type, constraints)

    return table_name, primary_key, columns


def parse_alter(sql_statement: str):
    alter_table_pattern = re.compile(
        r"ALTER TABLE\s+(?P<table_name>\w+)\s+(?P<action>ADD|DROP|MODIFY)\s+(?P<column_name>\w+)(?:\s+("
        r"?P<column_type>\w+\(?\d*\)?))?",
        re.IGNORECASE
    )

    match = alter_table_pattern.search(sql_statement)
    if match:
        # 提取信息
        table_name = match.group("table_name")
        action = match.group("action").upper()
        column_name = match.group("column_name")
        column_type = match.group("column_type") if match.group("column_type") else None
        return table_name, action, column_name, column_type
    else:
        raise ValueError("Invalid ALTER TABLE statement.")


def parse_drop(sql_statement: str) -> str:
    drop_table_pattern = re.compile(
        r"DROP TABLE\s+(?P<table_name>\w+)",
        re.IGNORECASE
    )

    match = drop_table_pattern.search(sql_statement)
    if match:
        # 提取表名
        table_name = match.group("table_name")
        return table_name
    else:
        raise ValueError("Invalid DROP TABLE statement.")


def parse_insert(sql_statement: str):
    # 正则表达式匹配 INSERT INTO 语句，支持多个 VALUES 插入
    insert_pattern = re.compile(
        r"INSERT INTO\s+(?P<table_name>\w+)\s*\((?P<columns>[\w\s,]+)\)\s*VALUES\s*(?P<values_group>\([^)]+\)(?:\s*,"
        r"\s*\([^)]+\))*)",
        re.IGNORECASE
    )

    match = insert_pattern.search(sql_statement)
    if match:
        table_name = match.group("table_name")
        columns = [col.strip() for col in match.group("columns").split(",")]

        # 提取多个 VALUES 块
        values_group = match.group("values_group")
        # 分割多个插入的值，每个插入的值都是一个括号内的字符串
        values_list = re.findall(r"\(([^)]+)\)", values_group)

        parsed_values_list = []

        for values in values_list:
            # 将每条 VALUES 的各项值分割并去除多余的空格
            values = values.split(",")
            parsed_values = {}

            # 将列名和对应的值匹配，并转换为合适的数据类型
            for col, value in zip(columns, values):
                value = value.strip()
                if value.startswith("'") and value.endswith("'"):
                    # 字符类型，去掉引号
                    parsed_values[col] = value[1:-1]
                else:
                    # 尝试转换为数字类型
                    try:
                        if '.' in value:
                            parsed_values[col] = float(value)  # 浮点数
                        else:
                            parsed_values[col] = int(value)  # 整数
                    except ValueError:
                        parsed_values[col] = value  # 保持原样

            # 将解析后的值添加到列表中
            parsed_values_list.append(parsed_values)

        return table_name, parsed_values_list
    else:
        raise ValueError("Invalid INSERT statement.")


def parse_update(sql_statement: str):
    # 正则表达式匹配 UPDATE 语句
    update_pattern = re.compile(
        r"UPDATE\s+(?P<table_name>\w+)\s+SET\s+(?P<set_clause>[\w\s=,'\"\.]+)\s+WHERE\s+(?P<condition>.+)",
        re.IGNORECASE
    )

    # 匹配 SQL 语句
    match = update_pattern.search(sql_statement)
    if match:
        table_name = match.group("table_name")

        # 提取 SET 子句并构建字典
        set_clause = match.group("set_clause")
        set_items = [item.strip() for item in set_clause.split(",")]
        updated_values = {}

        for item in set_items:
            col_name, new_value = item.split("=")
            updated_values[col_name.strip()] = new_value.strip().strip("'").strip('"')  # 去掉引号

        # 提取 WHERE 条件
        condition = match.group("condition").strip()
        return table_name, updated_values, condition
    else:
        raise ValueError("Invalid UPDATE statement.")


def parse_select(sql_statement: str) -> dict:
    select_pattern = re.compile(
        r"SELECT\s+(?P<columns>[\w\s,*\-]+)\s+FROM\s+(?P<tables>\w+)"
        r"(?P<joins>(?:\s+JOIN\s+\w+\s+ON\s+[\w.]+\s*(?:=|==)\s*[\w.]+)*)"
        r"(?:\s+WHERE\s+(?P<where_condition>[^;]*))?",
        re.IGNORECASE
    )

    # 匹配 SQL 语句
    match = select_pattern.search(sql_statement)
    if not match:
        raise ValueError("Invalid SELECT statement.")

    # 提取基本字段
    columns = [col.strip() for col in match.group("columns").split(',')] if match.group("columns") else None
    tables = [table.strip() for table in match.group("tables").split(',')] if match.group("tables") else None
    where_condition = match.group("where_condition").strip() if match.group("where_condition") else None

    # 提取 JOIN 信息，可能有多个
    joins_text = match.group("joins")
    joins = []
    if joins_text:
        join_pattern = re.compile(r"JOIN\s+(?P<table>\w+)\s+ON\s+(?P<condition>[\w.]+\s*(?:=|==)\s*[\w.]+)",
                                  re.IGNORECASE)
        join_matches = join_pattern.findall(joins_text)
        for join_table, join_condition in join_matches:
            joins.append({
                "join_table": join_table,
                "join_condition": join_condition
            })

    return {
        "columns": columns,
        "tables": tables,
        "joins": joins if joins else None,
        "where": where_condition,
    }


if __name__ == '__main__':
    sql = """select * from employee join department on employee.dno = department.dnumber join depart_location on depart_location.dnumber = employee.dno where dname=='研发部'"""
    parsed_sql = parse_select(sql)
    print(parsed_sql)
