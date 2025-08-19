import pymysql
import re
import json

def get_mysql_connection(host, user, password, port=3306, db=None):
    """
    Establish MySQL connection
    """
    conn = None
    try:
        conn = pymysql.connect(
            host=host,
            user=user,
            password=password,
            port=port,
            db=db,
            charset='utf8mb4'
        )
    except Exception as e:
        print(f"Failed to connect to MySQL: {e}")
    return conn

def fetch_table_schemas(host, user, password, port=3306):
    """
    Execute SQL and return result list
    """
    sql = """
    select table_schema 
    from information_schema.tables 
    where table_schema not in ('mysql','information_schema','__internal_schema') 
    group by table_schema ;
    """
    result_list = []
    conn = get_mysql_connection(host, user, password, port)
    if conn:
        try:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                rows = cursor.fetchall()
                result_list = [row[0] for row in rows]
        except Exception as e:
            print(f"Failed to execute SQL: {e}")
        finally:
            conn.close()
    return result_list

def clean_dbname(dbname):
    """
    Remove 'default_cluster:' prefix if exists
    """
    if dbname and isinstance(dbname, str):
        if dbname.startswith("default_cluster:"):
            return dbname.split(":", 1)[1]
    return dbname

def fetch_routine_load_info(host, user, password, db_list, port=3306, filter_db=None, filter_table=None):
    """
    Traverse db_list, use each db, execute show routine load; get Name,DbName,TableName,Progress fields
    Support filter_db, filter_table, only get routine load for specified db/table
    Return format: {db1: [dict1, dict2, ...], db2: [...], ...}
    """
    result = {}
    for db in db_list:
        if filter_db and db != filter_db:
            continue
        conn = get_mysql_connection(host, user, password, port, db=db)
        db_result = []
        if conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute("show routine load;")
                    columns = [desc[0] for desc in cursor.description]
                    rows = cursor.fetchall()
                    for row in rows:
                        row_dict = dict(zip(columns, row))
                        filtered = {k: row_dict.get(k) for k in ['Name', 'DbName', 'TableName', 'Progress']}
                        filtered['DbName'] = clean_dbname(filtered.get('DbName'))
                        if filter_table and filtered.get('TableName') != filter_table:
                            continue
                        db_result.append(filtered)
            except Exception as e:
                print(f"Failed to execute show routine load in db {db}: {e}")
            finally:
                conn.close()
        if db_result:
            result[db] = db_result
    return result

def get_dbtable_to_names(routine_load_infos):
    """
    Count all Names for each DbName.TableName
    routine_load_infos: {db1: [dict1, dict2, ...], db2: [...], ...}
    Return: { "DbName.TableName": set([Name1, Name2, ...]), ... }
    """
    dbtable_to_names = {}
    for db, info_list in routine_load_infos.items():
        for item in info_list:
            dbname = clean_dbname(item.get('DbName'))
            tablename = item.get('TableName')
            name = item.get('Name')
            if dbname and tablename and name:
                key = f"{dbname}.{tablename}"
                if key not in dbtable_to_names:
                    dbtable_to_names[key] = set()
                dbtable_to_names[key].add(name)
    return dbtable_to_names

def fetch_create_routine_load(host, user, password, port, db, routine_load_name):
    """
    Get create statement for specified routine load
    show create routine load for db.routineloadName
    Note: The third column is the create statement
    """
    db_clean = clean_dbname(db)
    conn = get_mysql_connection(host, user, password, port, db=db_clean)
    create_sql = ""
    if conn:
        try:
            with conn.cursor() as cursor:
                sql = f"show create routine load for {db_clean}.{routine_load_name}"
                cursor.execute(sql)
                row = cursor.fetchone()
                if row:
                    create_sql = row[2] if len(row) > 2 else (row[1] if len(row) > 1 else row[0])
        except Exception as e:
            print(f"Failed to get create statement for {db_clean}.{routine_load_name}: {e}")
        finally:
            conn.close()
    return create_sql

def replace_kafka_offsets(create_sql, progress_json):
    """
    Replace kafka_offsets in create_sql with offsets from progress_json, add 1 to each
    """
    try:
          # Check if progress_json is already a string (not JSON)
        if isinstance(progress_json, str) and not progress_json.startswith('{'):
            print(f"Progress is a string, skipping kafka_offsets replacement: {progress_json}")
            return create_sql
            
        progress = json.loads(progress_json)
        offsets = []
        for k in sorted(progress.keys(), key=lambda x: int(x)):
            v = int(progress[k]) + 1
            offsets.append(str(v))
        new_offsets = ", ".join(offsets)
        def repl(m):
            return f'kafka_offsets" = "{new_offsets}"'
        create_sql_new = re.sub(r'kafka_offsets"\s*=\s*"[0-9,\s]*"', repl, create_sql)
        return create_sql_new
    except Exception as e:
        print(f"Failed to replace kafka_offsets: {e}")
        return create_sql

def patch_create_sql_dbname(create_sql, dbname, routinename):
    """
    Replace CREATE ROUTINE LOAD routinename ON with CREATE ROUTINE LOAD dbname.routinename ON
    """
    pattern = r'(CREATE\s+ROUTINE\s+LOAD\s+)(\w+)(\s+ON\s+)'
    replacement = r'\1' + f'{dbname}.{routinename}' + r'\3'
    create_sql = re.sub(pattern, replacement, create_sql, count=1)
    return create_sql

def patch_group_id(create_sql):
    """
    Replace "property.group.id" = "xxx" with "property.group.id" = "xxx_new"
    """
    def repl(m):
        old_value = m.group(1)
        if old_value.endswith("_new"):
            return m.group(0)
        return f'"property.group.id" = "{old_value}_new"'
    create_sql = re.sub(r'"property\.group.id"\s*=\s*"([^"]+)"', repl, create_sql)
    return create_sql

def split_sql_with_separator(sql):
    """
    Add separator after each );
    """
    return re.sub(r'\);\s*', ');\n----------------------------------\n', sql)

def get_routine_load_create_sqls_with_offsets(host, user, password, port, routine_load_infos):
    """
    Get create statement for each routine load, replace kafka_offsets, patch dbname, patch group id
    Return: {db: {routine_load_name: create_sql}}
    """
    result = {}
    for db, info_list in routine_load_infos.items():
        db_result = {}
        for item in info_list:
            name = item.get('Name')
            dbname = clean_dbname(item.get('DbName'))
            progress = item.get('Progress')
            if not (name and dbname and progress):
                continue
            create_sql = fetch_create_routine_load(host, user, password, port, dbname, name)
            if not create_sql:
                continue
            create_sql_new = replace_kafka_offsets(create_sql, progress)
            create_sql_new = patch_create_sql_dbname(create_sql_new, dbname, name)
            create_sql_new = patch_group_id(create_sql_new)
            db_result[name] = create_sql_new
        if db_result:
            result[db] = db_result
    return result

def generate_pause_resume_sql(dbtable_to_names, filter_db=None, filter_table=None):
    """
    Generate PAUSE/RESUME ROUTINE LOAD statements
    dbtable_to_names: { "DbName.TableName": set([Name1, Name2, ...]), ... }
    filter_db, filter_table: only generate statements for specified db/table
    Return: list of (dbtable, [names], [pause_sqls], [resume_sqls])
    """
    result = []
    for dbtable, names in dbtable_to_names.items():
        db, table = dbtable.split('.', 1)
        if filter_db and db != filter_db:
            continue
        if filter_table and table != filter_table:
            continue
        names_sorted = sorted(list(names))
        pause_sqls = []
        resume_sqls = []
        for name in names_sorted:
            pause_sqls.append(f"PAUSE ROUTINE LOAD FOR {db}.{name};")
            resume_sqls.append(f"RESUME ROUTINE LOAD FOR {db}.{name};")
        result.append((dbtable, names_sorted, pause_sqls, resume_sqls))
    return result

def main():
    print("Please input MySQL connection info:")
    host = input("host: ")
    user = input("user: ")
    password = input("password: ")
    port = input("port(default 3306): ")
    port = int(port) if port else 3306

    print("Please select mode:")
    print("1. Show routine load names for each table and their PAUSE/RESUME statements")
    print("2. Show modified routine load create statements")
    mode = input("Input mode number(1/2): ").strip()

    filter_db = input("If you want to specify db name, input it (or press Enter): ").strip()
    filter_table = input("If you want to specify table name, input it (or press Enter): ").strip()
    filter_db = filter_db if filter_db else None
    filter_table = filter_table if filter_table else None

    db_list = fetch_table_schemas(host, user, password, port)
    routine_load_infos = fetch_routine_load_info(host, user, password, db_list, port, filter_db, filter_table)
    dbtable_to_names = get_dbtable_to_names(routine_load_infos)

    if mode == "1":
        result = generate_pause_resume_sql(dbtable_to_names, filter_db=filter_db, filter_table=filter_table)
        for dbtable, names, pause_sqls, resume_sqls in result:
            # 展示表名: routine load名字（逗号分隔）
            print(f"{dbtable}: {', '.join(names)}")
            # 如果只有一个routine load
            if len(names) == 1:
                print(pause_sqls[0])
                print(resume_sqls[0])
            else:
                for sql in pause_sqls:
                    print(sql)
                for sql in resume_sqls:
                    print(sql)
            print("")  # 空行分隔
    elif mode == "2":
        print("Getting routine load create statements...")
        create_sqls_dict = get_routine_load_create_sqls_with_offsets(host, user, password, port, routine_load_infos)
        all_sqls = []
        for db, name_sqls in create_sqls_dict.items():
            for name, sql in name_sqls.items():
                all_sqls.append(sql.strip())
        print("Modified routine load create statements:")
        print("\n----------------------------------\n".join(all_sqls))
    else:
        print("Invalid mode number.")

if __name__ == "__main__":
    main()
