import datetime as dt
from dataclasses import dataclass
from typing import Any, Dict, List, Type
from dataclasses_json import dataclass_json
from db_api import DBTable, DBField, DataBase, SelectionCriteria, DB_ROOT

import os
import json
import shutil


@dataclass_json
@dataclass
class DBField(DBField):
    name: str
    type: Type

    def __init__(self, name, type_):
        self.name = name
        self.type = type_


@dataclass_json
@dataclass
class SelectionCriteria(SelectionCriteria):
    field_name: str
    operator: str
    value: Any

    def __init__(self, field_name, operator, value):
        self.field_name = field_name
        self.operator = operator
        self.value = value


def get_str(param, operator, value):
    if isinstance(param, str):
        result = f"\"{param}\"" + operator
    else:
        result = str(param) + operator

    if isinstance(value, str):
        return result + f"\"{value}\""
    else:
        return result + str(value)

    # return f"\"{param[:-1]}\"" + operator + f"\"{value}\""


@dataclass_json
@dataclass
class DBTable(DBTable):
    name: str
    fields: List[DBField]
    key_field_name: str

    def __init__(self, name: str, fields: List[DBField], key_field_name: str):
        if key_field_name not in [field.name for field in fields]:
            raise ValueError
        self.key_field_name = key_field_name
        self.fields = fields
        self.name = name
        if not os.path.isfile(f"{DB_ROOT}/{self.name}/{self.name}{1}.json"):
            with open(f"{DB_ROOT}/{self.name}/{self.name}{1}.json", "w", encoding='utf-8') as data_file:
                json.dump({}, data_file)

    def add_count(self, count):
        if count != 0:
            with open(f"{DB_ROOT}/{self.name}/{self.name}.json", encoding='utf-8') as file:
                file_information = json.load(file)

            file_information["len"] += count

            with open(f"{DB_ROOT}/{self.name}/{self.name}.json", "w", encoding='utf-8') as file:
                json.dump(file_information, file)

    # TODO: ijson
    def count(self) -> int:
        with open(f"{DB_ROOT}/{self.name}/{self.name}.json", encoding='utf-8') as file:
            file_information = json.load(file)

        return file_information["len"]

    def insert_record(self, values: Dict[str, Any]) -> None:
        # check the correctness of the keys
        if self.key_field_name not in values.keys():
            raise KeyError("the key value didn't given")

        dates = [key for key, field in values.items() if isinstance(field, dt.datetime)]
        for date in dates:
            values[date] = values[date].strftime('%m%d%Y')

        insert_file = 1
        insert_data = {}
        num = 1
        first = True
        while os.path.isfile(f"{DB_ROOT}/{self.name}/{self.name}{num}.json"):
            with open(f"{DB_ROOT}/{self.name}/{self.name}{num}.json", encoding='utf-8') as file:
                my_data = json.load(file)

            for key in my_data.keys():
                if key == str(values[self.key_field_name]):
                    raise ValueError

            if len(my_data) < 1000 and first:
                first = False
                insert_file = num
                insert_data = my_data

            num += 1

        if first:
            with open(f"{DB_ROOT}/{self.name}/{self.name}{num}.json", "w", encoding='utf-8') as file:
                json.dump({}, file)
            insert_file = num
            insert_data = {}

        insert = values.pop(self.key_field_name)

        insert_data.update({insert: values})

        with open(f"{DB_ROOT}/{self.name}/{self.name}{insert_file}.json", "w", encoding='utf-8') as file:
            json.dump(insert_data, file)

        self.add_count(1)

    def delete_record(self, key: Any) -> None:
        num = 1
        while os.path.isfile(f"{DB_ROOT}/{self.name}/{self.name}{num}.json"):
            with open(f"{DB_ROOT}/{self.name}/{self.name}{num}.json", encoding='utf-8') as f:
                my_data = json.load(f)

            if str(key) in my_data.keys():
                my_data.pop(str(key))
                with open(f"{DB_ROOT}/{self.name}/{self.name}{num}.json", "w", encoding='utf-8') as file:
                    json.dump(my_data, file, ensure_ascii=False)
                self.add_count(-1)
                return
            num += 1

        raise ValueError

    def get_operation(self, criteria: List[SelectionCriteria], key, row):
        fields = [field.name for field in self.fields]
        operation = ""
        for criterion in criteria:
            if criterion.operator == "=":
                criterion.operator = "=="

            if criterion.field_name == self.key_field_name:
                key_type = [field.type for field in self.fields if self.key_field_name == field.name][0]
                if isinstance(key_type, str):
                    operation += get_str(key, criterion.operator, criterion.value) + " and "
                # TODO: int
                #  operation += get_str(get_type(key_type)(key), criterion.operator, criterion.value)

                else:
                    operation += get_str(int(key), criterion.operator, criterion.value) + " and "

            else:
                if criterion.field_name in fields:
                    operation += get_str(row[criterion.field_name], criterion.operator, criterion.value) + " and "

        return operation[:-4]

    def delete_records(self, criteria: List[SelectionCriteria]) -> None:
        num = 1

        with open(f"{DB_ROOT}/{self.name}/{self.name}{num}.json", encoding='utf-8') as f:
            my_data = json.load(f)

        count = 0
        deleted_keys = []

        for key, item in my_data.items():
            if eval(self.get_operation(criteria, key, item)):
                deleted_keys.append(key)
                count += 1

        self.add_count(-count)

    # TODO: ijson
    def get_record(self, key: Any) -> Dict[str, Any]:
        result = {"id": key}
        num = 1
        while os.path.isfile(f"{DB_ROOT}/{self.name}/{self.name}{num}.json"):
            with open(f"{DB_ROOT}/{self.name}/{self.name}{num}.json", encoding='utf-8') as f:
                my_data = json.load(f)

            if str(key) in my_data.keys():
                result.update(my_data[str(key)])
                return result

            num += 1

        raise KeyError

    def update_record(self, key: Any, values: Dict[str, Any]) -> None:
        num = 1
        while os.path.isfile(f"{DB_ROOT}/{self.name}/{self.name}{num}.json"):
            with open(f"{DB_ROOT}/{self.name}/{self.name}{num}.json", encoding='utf-8') as f:
                my_data = json.load(f)

            if str(key) in my_data.keys():
                for item in values.keys():
                    my_data[str(key)][item] = values[item]

                with open(f"{DB_ROOT}/{self.name}/{self.name}{num}.json", "w", encoding='utf-8') as file:
                    json.dump(my_data, file, ensure_ascii=False)
                return

            num += 1

    def query_table(self, criteria: List[SelectionCriteria]) \
            -> List[Dict[str, Any]]:
        result = []
        num = 1
        while os.path.isfile(f"{DB_ROOT}/{self.name}/{self.name}{num}.json"):
            with open(f"{DB_ROOT}/{self.name}/{self.name}{num}.json", encoding='utf-8') as f:
                my_data = json.load(f)

            for key, value in my_data.items():
                if eval(self.get_operation(criteria, key, value)):
                    new_data = {self.key_field_name: key}
                    new_data.update(value)
                    result.append(new_data)

                num += 1

        return result

    def create_index(self, field_to_index: str) -> None:
        raise NotImplementedError


def get_type(data):
    if data == "<class 'int'>":
        return int
    if data == "<class 'str'>":
        return str
    else:
        return dt.datetime


@dataclass_json
@dataclass
class DataBase(DataBase):
    # Put here any instance information needed to support the API
    my_tables: Dict[str, DBTable]

    def __init__(self):
        self.my_tables = {}

    def create_table(self,
                     table_name: str,
                     fields: List[DBField],
                     key_field_name: str) -> DBTable:
        if not os.path.isdir(f"{DB_ROOT}/{table_name}"):
            try:
                os.makedirs(f"{DB_ROOT}/{table_name}")
                new_table = DBTable(table_name, fields, key_field_name)

                with open(f"{DB_ROOT}/{table_name}/{table_name}.json", "w") as tables:
                    json.dump({"len": 0,
                               "name": table_name,
                               "fields": [{field.name: str(field.type)} for field in fields],
                               "key_field_name": key_field_name}, tables)

            except ValueError:
                self.delete_table(table_name)
                raise ValueError

            self.my_tables.update({table_name: new_table})
            return new_table

        raise FileExistsError

    def num_tables(self) -> int:
        return len([name for name in os.listdir(DB_ROOT) if os.path.isdir(os.path.join(DB_ROOT, name))])

    def get_table(self, table_name: str) -> DBTable:
        if table_name in self.my_tables.keys():
            return self.my_tables[table_name]

        if os.path.isdir(f"{DB_ROOT}/{table_name}"):
            with open(f"{DB_ROOT}/{table_name}/{table_name}.json") as tables:
                table_data = json.load(tables)
                new_table = DBTable(table_data["name"],
                                    [DBField(list(item.keys())[0], list(item.values())[0]) for item in table_data["fields"]],
                                    table_data["key_field_name"])
                self.my_tables[table_name] = new_table
                return new_table
        raise NameError

    def delete_table(self, table_name: str) -> None:
        if os.path.isdir(f"{DB_ROOT}/{table_name}"):
            shutil.rmtree(f"{DB_ROOT}/{table_name}", ignore_errors=True)
            if table_name in self.my_tables.keys():
                self.my_tables.pop(table_name)

    def get_tables_names(self) -> List[Any]:
        return os.listdir(DB_ROOT)

    def query_multiple_tables(
            self,
            tables: List[str],
            fields_and_values_list: List[List[SelectionCriteria]],
            fields_to_join_by: List[str]
    ) -> List[Dict[str, Any]]:
        raise NotImplementedError
