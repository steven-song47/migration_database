from collections import Counter
import yaml, psycopg2, openpyxl, os


class Read_YAML:

    def __init__(self, path):
        self.path = path
        yaml.warnings({'YAMLLoadWarning': False})

    def get_config(self):
        stream = open(self.path, mode="r", encoding="utf-8")
        data = yaml.load(stream)
        stream.close()
        return data


class Postgres_Operation:

    def __init__(self, host, port, user, psw, db):
        self.db = db
        self.connect = psycopg2.connect(database=db, host=host, user=user, password=psw, port=port)
        self.cur = self.connect.cursor()
        self.table_info = dict()

    def get_all_tables(self):
        table_list = list()
        self.cur.execute("select * from pg_tables;")
        for table in self.cur.fetchall():
            if table[0] == "public":
                table_list.append(table[1])
        return table_list

    def get_table_sum(self, table_list):
        sum_data = list()
        for table in table_list:
            self.cur.execute("select count(*) from %s" % table)
            table_total = self.cur.fetchall()
            if table_total:
                sum_data.append([self.db, table, table_total[0][0]])
        return sum_data

    def set_table_info(self, user_data, slice, choose):
        for table in user_data:
            total = self.get_table_total(table)
            user_data[table]['total_data'] = total
            title = self.get_table_title(table)
            user_data[table]['table_fields'] = title
            data = self.get_table_data(table, slice, choose)
            user_data[table]['table_data'] = data
        return user_data

    def get_table_total(self, table):
        total = 0
        self.cur.execute("select count(*) from %s" % table)
        table_total = self.cur.fetchall()
        if table_total:
            total = table_total[0][0]
        return total

    def get_table_title(self, table):
        title_list = list()
        self.cur.execute("select * from information_schema.columns where table_schema='public' and table_name='%s'" % table)
        result = self.cur.fetchall()
        if result:
            title_list = result
        return title_list

    def get_table_data(self, table, slice, choose):
        table_tmp_data = list()
        sum = self.get_table_total(table)
        for index in range(sum//slice + 1):
            origin = sum // slice * slice
            self.cur.execute("select * from %s limit %d OFFSET %d" % (table, choose, origin))
            result = self.cur.fetchall()
            table_tmp_data += result
        return table_tmp_data


class Excel_Operation:

    def __init__(self):
        self.file_name = "data_migration_config.xlsx"

    def check_file_exist(self):
        result = os.path.exists(self.file_name)
        return result

    def remove_file(self):
        os.remove(self.file_name)

    def create_excel(self, data):
        table_title = ["source_db", "source_table", "source_total", "target_db", "target_table", "migration", "split", \
                       "target_total", "total_check", "fields_check", "data_check"]
        if self.check_file_exist():
            self.remove_file()
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = "test config"
        for col in range(len(table_title)):
            c = col + 1
            ws.cell(row=1, column=c).value = table_title[col]
        for row in range(len(data)):
            ws.append(data[row])
        wb.save(filename=self.file_name)

    def read_excel(self):
        data = list()
        wb = openpyxl.load_workbook(self.file_name)
        sheets = wb.get_sheet_names()
        sheet_first = sheets[0]
        ws = wb.get_sheet_by_name(sheet_first)
        rows = ws.rows
        for row in rows:
            line = [col.value for col in row]
            data.append(line)
        return data

    def write_excel(self, origin_data, result_data):
        data = list()
        wb = openpyxl.Workbook()
        ws = wb.active
        for index, line_data in enumerate(origin_data):
            if index != 0:
                new_line_data = line_data[:4] + result_data[index-1]
                data.append(new_line_data)
            else:
                data.append(line_data)
        for row in range(len(data)):
            ws.append(data[row])
        wb.save(filename=self.file_name)


def create_config_excel():
    yaml = Read_YAML("config.yaml")
    source_db = yaml.get_config()['sql_config']['source_db']
    database = source_db["database"]
    host = source_db["host"]
    user = source_db["user"]
    password = source_db["password"]
    port = source_db["port"]
    p = Postgres_Operation(db=database, host=host, user=user, psw=password, port=port)
    data = p.get_table_sum(p.get_all_tables())
    excel = Excel_Operation()
    excel.create_excel(data)


def check_data_migration():
    yaml = Read_YAML("config.yaml")

    slice = yaml.get_config()['search_config']['slice']
    choose = yaml.get_config()['search_config']['choose']

    source_db = yaml.get_config()['sql_config']['source_db']
    database = source_db["database"]
    host = source_db["host"]
    user = source_db["user"]
    password = source_db["password"]
    port = source_db["port"]
    p_source = Postgres_Operation(db=database, host=host, user=user, psw=password, port=port)

    target_db = yaml.get_config()['sql_config']['source_db']
    database = target_db["database"]
    host = target_db["host"]
    user = target_db["user"]
    password = target_db["password"]
    port = target_db["port"]
    p_target = Postgres_Operation(db=database, host=host, user=user, psw=password, port=port)

    excel = Excel_Operation()
    user_data = excel.read_excel()
    source_data = dict()
    target_data = dict()
    for index, line_data in enumerate(user_data):
        if index:
            source_data[line_data[1]] = {
                "source_db": line_data[0],
                "is_migration": line_data[5],
                "is_splice": line_data[6],
                "total_data": 0,
                "table_data": list(),
                "table_fields": list(),
                "target_db": line_data[3],
                "target_table": line_data[4],
                "total_check": "",
                "fields_check": "",
                "data_check": "",
                "target_total": "",
            }
            target_data[line_data[4]] = {
                "is_migration": line_data[5],
                "is_splice": line_data[6],
                "total_data": 0,
                "table_data": list(),
                "table_fields": list()
            }
    source_data = p_source.set_table_info(source_data, slice, choose)
    target_data = p_target.set_table_info(target_data, slice, choose)
    result_data = list()
    for table in source_data:
        target_table = source_data[table]['target_table']
        is_migration = source_data[table]['is_migration']
        if is_migration == "Y":
            source_data[table]['target_total'] = target_data[target_table]['total_data']
            if source_data[table]['total_data'] == target_data[target_table]['total_data']:
                source_data[table]['total_check'] = "PASS"
            else:
                source_data[table]['total_check'] = "FAIL"
            source_title = Counter(source_data[table]['table_fields'])
            target_title = Counter(target_data[target_table]['table_fields'])
            if dict(source_title) == dict(target_title):
                source_data[table]['fields_check'] = "PASS"
            else:
                source_data[table]['fields_check'] = "FAIL"
            if source_data[table]['table_data'] == target_data[target_table]['table_data']:
                source_data[table]['data_check'] = "PASS"
            else:
                source_data[table]['data_check'] = "FAIL"
        result_data.append([source_data[table]['source_db'], table, source_data[table]['total_data'], source_data[table]['target_db'],\
                            target_table, source_data[table]['is_migration'], source_data[table]['is_splice'],\
                            source_data[table]['target_total'], source_data[table]['total_check'],\
                            source_data[table]['fields_check'], source_data[table]['data_check']])
        excel.create_excel(result_data)


if __name__ == '__main__':
    # Background: In the process of database migration testing, if we manually verify the data integrity in the source
    # database and target database, it will take a lot of time, but if we can automate this process, it will improve
    # the testing efficiency
    # Step 1: to create the testing configure file to let the users to input the key parameters.
    # The first column is the name of the source database.
    # The second one is the table name of the source database. And all the tables in the source database will be listed
    # in the file.
    # The third one shows the total amount of data for each table.
    # And users will need to fill out columns 4 through 7.
    # target_db: the name of the target database.
    # target_table: the table in target Databse corresponding to source table.
    # migration: whether to migrate data. input Y or N into this column.
    # split: the reserved field
    # create_config_excel()
    # Step 2: using the configured file to test the data of different database and rewrite the testing result into the
    # Excel file.
    # total_check: Check whether the total amount of data in different tables is consistent.
    # fields_check: Check whether every fields in different tables is consistent. Includes field types, and so on.
    # data_check: Check whether all data in different tables is consistent.
    check_data_migration()
    # Due to time constraints, this round of development only takes 2-3 days to develop, so the overall functionality
    # is limited. In the future, I will consider adding support for more complex database table migration tests and
    # providing a testing report.
