from db_api import DBField, List, Dict, Any, SelectionCriteria, DB_ROOT
import db_api
import json
import os
import nltk

from nltk.corpus import stopwords
from stop_words import get_stop_words
from nltk.stem import PorterStemmer
from string import ascii_lowercase, digits, punctuation

class DBTable(db_api.DBTable):
    # name: str
    # fields: List[DBField]
    # key_field_name: str

    def __init__(self, name: str, fields: List[DBField], key_field_name: str):
        super().__init__(name, fields, key_field_name)
        self.key_index_path = f'{DB_ROOT}\\{name}_key_index.json'
        self.key_index = []
        self.current_table_path = ""
        self.current_table = []

        self.load_db()

    def load_key_index(self):
        if os.path.exists(self.key_index_path):
            the_file = open(self.key_index_path, encoding="utf8")
            self.key_index = json.load(the_file)
            the_file.close()
        else:
            self.key_index.append({})
            self.key_index.append({"list_of_table_pathes": [], "file_amount": 0, "fields": self.fields, "key_field_name": self.key_field_name, "name": self.name, "indexes" : {}})
            self.update_key_index()

    def load_current_table(self):
        if self.key_index[1]["file_amount"] > 0:
            the_file = open(self.key_index[1]["list_of_table_pathes"][-1], encoding="utf8")
            self.current_table = json.load(the_file)
            the_file.close()

    def load_db(self):
        self.load_key_index()
        self.load_current_table()

    def count(self) -> int:
        count = 0
        for table_path in self.key_index[1]["list_of_table_pathes"]:
            the_file = open(table_path, encoding="utf8")
            data_table = json.load(the_file)
            the_file.close()
            count += len(data_table[0])
        return count

    def update_key_index(self):
        the_file = open(self.key_index_path, 'w+', encoding="utf8")
        the_file.write(json.dumps(self.key_index, default=str))
        the_file.close()

    def update_current_table(self):
        the_file = open(self.current_table_path, 'w+', encoding="utf8")
        the_file.write(json.dumps(self.current_table, default=str))
        the_file.close()

    def update_index(self, field, key, value_of_the_field, index_type):
        if index_type == "text_index":
            self.update_text_index(field, key, value_of_the_field)
        if index_type == "hash_index":
            self.update_hash_index(field, key, value_of_the_field)

    def update_hash_index(self, field, key, value_of_the_field):
        with open(self.key_index[1]["indexes"][field][0], 'r+') as the_file:
            index = json.load(the_file)
            index[0][value_of_the_field].append(key)
            the_file.seek(0)
            json.dump(index, the_file, default=str)
            the_file.truncate()

    def update_text_index(self, field, key, value_of_the_field):
        with open(self.key_index[1]["indexes"][field][0], 'r+') as the_file:
            index = json.load(the_file)
            words = value_of_the_field.split(" ")
            words = self.words_dilution(words)
            for word in words:
                index[0][word].append(key)
            the_file.seek(0)
            json.dump(index, the_file, default=str)
            the_file.truncate()

    @staticmethod
    def update_json_dict_file(path, key, values=0):
        with open(path, 'r+') as the_file:
            data = json.load(the_file)
            if values == 0:
                data[0].pop(str(key), ValueError)
            else:
                data[0][str(key)] = values
            the_file.seek(0)
            json.dump(data, the_file, default=str)
            the_file.truncate()

    def insert_record(self, values: Dict[str, Any]) -> None:
        if self.key_field_name in values.keys():
            if (self.key_index[1]["file_amount"] == 0 and len(self.current_table_path) == 0) or (len(self.current_table[0]) > 50) :
                self.current_table_path = f'{DB_ROOT}\\{self.name}_{self.key_index[1]["file_amount"] + 1}.json'
                self.current_table = []
                self.key_index[1]["list_of_table_pathes"].append(self.current_table_path)
                self.key_index[1]["file_amount"] += 1
            # update key_index
            if str(values[self.key_field_name]) in self.key_index[0]:
                raise ValueError
            self.key_index[0][str(values[self.key_field_name])] = self.current_table_path
            # update DB
            if len(self.current_table) == 0:
                self.current_table.append({str(values[self.key_field_name]): values})
            else:
                self.current_table[0][str(values[self.key_field_name])] = values
            for field in values:
                if field in self.key_index[1]["indexes"]:
                    self.update_index(field, values[self.key_field_name], values[field], self.key_index[1]["indexes"][1])
            self.update_current_table()
            self.update_key_index()

    def remove_from_index(self, field, key, str):
        with open(self.key_index[1]["indexes"][field][0], 'r+') as the_file:
            index = json.load(the_file)
            for word in self.words_dilution(str.split(" ")):
                index[0][word].remove(key)
            the_file.seek(0)
            json.dump(index, the_file, default=str)
            the_file.truncate()

    def delete_record(self, key: Any) -> None:
        if str(key) not in self.key_index[0]:
            raise ValueError
        # delete from DB
        with open(self.key_index[0][str(key)], 'r+') as the_file:
            data = json.load(the_file)
            for field in data[0][str(key)]:
                if field in self.key_index[1]["indexes"]:
                    self.remove_from_index(field, key, data[0][str(key)], self.key_index[1]["indexes"][1])
            data[0].pop(str(key), ValueError)
            the_file.seek(0)
            json.dump(data, the_file, default=str)
            the_file.truncate()
        if str(key) in self.current_table[0]:
            self.current_table[0].pop(str(key))
        # delete from key_index
        self.key_index[0].pop(str(key))
        self.update_key_index()

    def delete_records(self, criteria: List[SelectionCriteria]) -> None:
        for i in range(len(criteria)):
            if criteria[i].operator == "=":
                criteria[i].operator = "=="
        list_of_deleted_keys = []
        for i, c in enumerate(criteria):
            if c.field_name in self.key_index[1]["indexes"]:
                if "hash_table" in self.key_index[1]["indexes"][c.field_name]:
                    with open(self.key_index[1]["indexes"][c.field_name][0], 'r+') as the_file:
                        index = json.load(the_file)
                    if c.operator == "==":
                        list_of_deleted_keys += index[0][c.field_name][c.value]
                    else:
                        list_of_deleted_keys += list(filter(lambda key: all(
                        [eval(self.get_record(key)[str(c.field_name)]) + c.operator + str(c.value) == True]), index[0][
                        c.field_name]))
                    criteria.pop(i)
        for table_path in self.key_index[1]["list_of_table_pathes"]:
            table_file = open(table_path)
            data_table = json.load(table_file)
            list_of_deleted_keys += list(filter(lambda x: all(
                [eval(str(data_table[0][x][criteria[i].field_name]) + criteria[i].operator + str(
                    criteria[i].value)) == True for i in range(len(criteria))]),
                                               list(data_table[0].keys())))
            # todo: improve afficiant
        for key in list_of_deleted_keys:
            self.delete_record(key)

    def get_record(self, key: Any) -> Dict[str, Any]:
        table_file = open(self.key_index[0][str(key)])
        data_table = json.load(table_file)
        table_file.close()
        return data_table[0][str(key)]

    def update_record(self, key: Any, values: Dict[str, Any]) -> None:
        for field in values:
            if field in self.key_index[1]["indexes"]:
                self.update_index(field, str(key), values[field],  self.key_index[1]["indexes"][1])
        with open(self.key_index[0][str(key)], 'r+') as table_file:
            data = json.load(table_file)
            data[0][str(key)].update(values)
            table_file.seek(0)
            json.dump(data, table_file, default=str)
            table_file.truncate()

    def query_table(self, criteria: List[SelectionCriteria]) -> List[Dict[str, Any]]:
        for i in range(len(criteria)):
            if criteria[i].operator == "=":
                criteria[i].operator = "=="
        list_of_returned_keys = []
        for i, c in enumerate(criteria):
            if c.field_name in self.key_index[1]["indexes"]:
                if "hash_table" in self.key_index[1]["indexes"][c.field_name]:
                    with open(self.key_index[1]["indexes"][c.field_name][0], 'r+') as the_file:
                        index = json.load(the_file)
                    if c.operator == "==":
                        list_of_returned_keys += index[0][c.field_name][c.value]
                    else:
                        list_of_returned_keys += list(filter(lambda key: all(
                        [eval(self.get_record(key)[str(c.field_name)]) + c.operator + str(c.value) == True]), index[0][
                        c.field_name]))
                    criteria.pop(i)
        for table_path in self.key_index[1]["list_of_table_pathes"]:
            with open(table_path) as table_file:
                data_table = json.load(table_file)
                list_of_returned_keys += list(filter(lambda x: all([eval(str(data_table[0][x][str(criteria[i].field_name)]) + criteria[i].operator + str(criteria[i].value)) == True for i in range(len(criteria))]), list(data_table[0].keys())))
        # todo: improve afficiant
        return [self.get_record(str(key)) for key in list_of_returned_keys]

    def create_hash_index(self, field_to_index: str):
        if field_to_index in self.key_index[1]["indexes"]:
            raise ValueError
        self.key_index[1]["indexes"][field_to_index] = []
        self.key_index[1]["indexes"][field_to_index].append(f'{DB_ROOT}\\{field_to_index}_hash_index.json')
        self.key_index[1]["indexes"][field_to_index].append("hash_index")
        index = []
        index.append({})
        for path in self.key_index[1]["list_of_table_pathes"]:
            with open(path, 'r+') as the_file:
                data = json.load(the_file)
                for key in data[0]:
                    if field_to_index in data[0][key]:
                        if data[0][key][field_to_index] not in index[0]:
                            index[0][data[0][key][field_to_index]] = []
                        index[0][data[0][key][field_to_index]].append(key)

        the_file = open(self.key_index[1]["indexes"][field_to_index][0], 'w+', encoding="utf8")
        the_file.write(json.dumps(index, default=str))
        the_file.close()

    def get_hash_index(self, field_to_index: str):
        if field_to_index not in self.key_index[1]["indexes"]:
            raise ValueError
        if self.key_index[1]["indexes"][field_to_index][1] != "hash_table":
            raise ValueError
        return self.key_index[1]["indexes"][field_to_index][0]

    def create_index(self, field_to_index: str, type) -> None:
        if type == "text_index":
            self.create_text_index(field_to_index)
        if type == "hash_index":
            self.create_hash_index(field_to_index)

    @staticmethod
    def words_dilution(words):
        exclude = punctuation
        ls=[]
        ps = PorterStemmer()
        for i,word in enumerate(words):
            word = ps.stem(word)
            str_input = (''.join(ch for ch in word if ch not in exclude)).lower()
            if str_input != "":
                ls.append(str_input)
        ls = list(set(ls))
        ls = [word for word in ls if word not in list(get_stop_words('en'))]
        return ls

    def find_in_text_index(self, field, words):
        words = self.words_dilution(words.split(" "))
        with open(self.key_index[1]["indexes"][field][0], 'r+') as the_file:
            index = json.load(the_file)
            for w in words:
                if w in index[0]:
                    _list = index[0][w]
                    for word in words:
                        if word in index[0]:
                           _list = set(_list) & set(index[0][word])
        return list(_list)

    def create_text_index(self, field_to_index: str) -> None:
        if field_to_index in self.key_index[1]["indexes"]:
            raise  ValueError
        self.key_index[1]["indexes"][field_to_index] = []
        self.key_index[1]["indexes"][field_to_index].append(f'{DB_ROOT}\\{field_to_index}_text_index.json')
        self.key_index[1]["indexes"][field_to_index].append("text_index")
        index = []
        index.append({})
        for path in self.key_index[1]["list_of_table_pathes"]:
            with open(path, 'r+') as the_file:
                data = json.load(the_file)
                for key in data[0]:
                    words = data[0][key][field_to_index].split(" ")
                    words = self.words_dilution(words)
                    for word in words:
                        if word not in index[0]:
                            index[0][word] = []
                        index[0][word].append(key)

        the_file = open(self.key_index[1]["indexes"][field_to_index][0], 'w+', encoding="utf8")
        the_file.write(json.dumps(index, default=str))
        the_file.close()






class DataBase(db_api.DataBase):
    # Put here any instance information needed to support the API
    def __init__(self):
        self.tables_dict_path = f'{DB_ROOT}\\tables.json'
        self.dict_of_tables = []
        self.load_tables()

    def load_tables(self):
        if os.path.exists(self.tables_dict_path):
            the_file = open(self.tables_dict_path, encoding="utf8")
            tmp = json.load(the_file)
            if len(tmp) > 0:
                self.dict_of_tables = tmp
            the_file.close()

    def update_tables_dict(self):
        the_file = open(self.tables_dict_path, 'w+', encoding="utf8")
        the_file.write(json.dumps(self.dict_of_tables, default=str))
        the_file.close()

    def create_table(self, table_name: str, fields: List[DBField], key_field_name: str) -> DBTable:
        if len(self.dict_of_tables) == 0:
            self.dict_of_tables.append({})
        if table_name in self.dict_of_tables[0]:
            raise ValueError
        if key_field_name not in list(map(lambda x: x.name , fields)):
            raise ValueError
        DB = DBTable(table_name, fields, key_field_name)
        self.dict_of_tables[0][table_name] = DB.key_index_path
        self.update_tables_dict()
        return DB

    def num_tables(self) -> int:
        if len(self.dict_of_tables) > 0:
            return len(self.dict_of_tables[0])
        return 0

    def get_table(self, table_name: str) -> DBTable:
        the_file = open(self.dict_of_tables[0][table_name], encoding="utf8")
        data = json.load(the_file)
        the_file.close()
        fields = data[1]["fields"]
        key_field_name = data[1]["key_field_name"]
        return DBTable(table_name, fields, key_field_name)

    def delete_table(self, table_name: str) -> None:
        self.dict_of_tables[0].pop(table_name)
        self.update_tables_dict()

    def get_tables_names(self) -> List[Any]:
        if len(self.dict_of_tables) > 0:
            return list(self.dict_of_tables[0].keys())
        return []


    def query_multiple_tables(self, tables: List[str], fields_and_values_list: List[List[SelectionCriteria]],
                              fields_to_join_by: List[str]) -> List[Dict[str, Any]]:
        table_dict = {}
        hash_table_dict = {}
        for table in tables:
            if table not in self.dict_of_tables[0]:
                raise ValueError
            table_dict[table] = self.get_table(table)
            for field in fields_to_join_by:
                table_dict[table].create_index(field, "hash_table")
                hash_table_dict[table][field] = table_dict[table].get_hash_index(field)
            if fields_to_join_by not in table_dict[table].field:
                raise ValueError

        return []



