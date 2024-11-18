from tabulate import tabulate
from mysql import YangSQL
from sql_parser import *


def sql_dialog():
    sql_engine = YangSQL()
    print("Welcome to use YangSQL! Type ';' at the end of the statement to execute it.")

    sql_buffer = []
    user_input = input('User/YangSQL> ')
    while True:
        sql_buffer.append(user_input.strip())

        if user_input.strip().endswith(';'):
            full_sql = " ".join(sql_buffer)
            sql_buffer = []
            full_sql = full_sql.rstrip(';')
            is_valid, _ = check_sql_syntax(full_sql)
            if is_valid:
                if full_sql.lower().startswith('create table'):
                    table_name, primary_key, columns = parse_create(full_sql)
                    sql_engine.create(table_name, primary_key, columns)

                elif full_sql.lower().startswith('alter table'):
                    table_name, action, column_name, column_type = parse_alter(full_sql)
                    sql_engine.alter(table_name, action, column_name, column_type)

                elif full_sql.lower().startswith('insert into'):
                    table_name, values = parse_insert(full_sql)
                    sql_engine.insert(table_name, values)

                elif full_sql.lower().startswith('update'):
                    table_name, values, where = parse_update(full_sql)
                    sql_engine.update(table_name, values, where)

                elif full_sql.lower().startswith('drop'):
                    sql_engine.drop(parse_drop(full_sql))

                elif full_sql.lower().startswith('select'):
                    res = sql_engine.select(parse_select(full_sql))
                    print(tabulate(res, headers='keys', tablefmt='pretty', showindex=False))

                else:
                    print(f"Sorry, the argument '{' '.join(full_sql.split()[:2])}' is not supported!")
                print('User/YangSQL> ')
            else:
                print("Invalid SQL syntax")
        user_input = input()

        if user_input.strip().lower() == 'exit':
            print("Exiting YangSQL.")
            break


if __name__ == '__main__':
    # try:
    #     sql_dialog()
    # except Exception as e:
    #     print(f"An error occurred: {e}")
    sql_dialog()
