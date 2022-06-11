import os
import json
import shutil
from textwrap import indent
import uuid

class PythonDb:
    def __init__(self) -> None:
        if "DATABASE" not in os.listdir("./"):
            os.makedirs("DATABASE")
            print("-*-*-*-*-* YOUR DATABASE HAS BEEN SUCCESSFULLY CREATED -*-*-*-*-*")

    def delete_db(self):
        if "DATABASE" in os.listdir("./"):
            if input("Are you sure that you want delete DATABASE ? (yes / no) : ").lower() == "yes" :
                shutil.rmtree("./DATABASE", ignore_errors=True)
                print("-*-*-*-*-* YOUR DATABASE HAS BEEN SUCCESSFULLY DELETED -*-*-*-*-*")
        else:
            print("Error : DATABESE Is Not Avalible")

    def create_index(self, index_name):
        self.index_name = index_name
        if f'{self.index_name}.json' not in os.listdir("./DATABASE/"):
            with open(f'./DATABASE/{self.index_name}.json', "w") as file:
                json.dump({"data":[]}, file, indent=4)
            print("-*-*-*-*-* YOUR INDEX HAS BEEN SUCCESSFULLY CREATED -*-*-*-*-*")

    def delete_index(self, index_name):
        self.index_name = index_name
        if f'{self.index_name}.json' in os.listdir("./DATABASE/"):
            if input("Are you sure that you want delete DATABASE ? (yes / no) : ").lower() == "yes" :
                os.remove(f'./DATABASE/{self.index_name}.json')
                print("-*-*-*-*-* YOUR INDEX HAS BEEN SUCCESSFULLY DELETED -*-*-*-*-*")
        else:
            print("Error : This INDEX Is Not Avalible")

    def rename_index(self, old_name, new_name):
        self.old_name = old_name
        self.new_name = new_name
        if f'{self.old_name}.json' in os.listdir("./DATABASE/"):
            if input("Are you sure that you want delete DATABASE ? (yes / no) : ").lower() == "yes" :
                os.rename(f'./DATABASE/{self.old_name}.json', f'./DATABASE/{self.new_name}.json')
                print("-*-*-*-*-* YOUR INDEX HAS BEEN SUCCESSFULLY RENAME -*-*-*-*-*")
        else:
            print("Error : This INDEX Is Not Avalible")

    def index(self, index_name, data, id=None, multi_data=False):
        self.data = data
        self.id = id
        self.multi_data = multi_data
        self.index_name = index_name
        if f'{self.index_name}.json' in os.listdir("./DATABASE/"):
            with open(f'./DATABASE/{self.index_name}.json', "r", encoding='utf-8') as file:
                self.object = json.loads(file.read())
            if "data" in self.object and type(self.object["data"] is list):
                if self.multi_data is not True:
                    if self.id is None:           #   ایجاد یک ایدی یونیک
                        self.id = uuid.uuid4()
                    else:                          #    چک برای نکراری نبودن ایدی
                        for doc in self.object["data"]:
                            if doc["id"] == self.id:
                                print("Warning : This Id Is Not Avalible")
                    if type(self.data) is dict:
                        self.object["data"].append({"id":str(self.id), "source":self.data})
                        self.object["index_name"] = self.index_name
                        return self.object
                    print("Error : Send Me A Json")
                else :
                    if self.id is None:
                        for sub_data in self.data:
                            self.id = uuid.uuid4()
                            if type(sub_data) is dict:
                                self.object["data"].append({"id":str(self.id), "source":sub_data})
                            else:
                                print("Error : Send Me A List Of Jsons")
                        self.object["index_name"] = self.index_name
                        return self.object
                    print("you can not send id for multi data index")
            else:
                print("Warning : DATABASE Have A Problem")
        else:
            print("Error : This INDEX Is Not Avalible")
        

    def save(self, object):
        self.object = object
        self.index_name = self.object["index_name"]
        self.object.pop("index_name")
        if f'{self.index_name}.json' in os.listdir("./DATABASE/"):
            with open(f'./DATABASE/{self.index_name}.json', "w") as file:
                json.dump(self.object, file, indent=4)
        else:
            print("Error : This INDEX Is Not Avalible")

    def add_relation(self, target_index, base_index, target_field):
        self.target_index = target_index
        self.base_index = base_index
        self.target_field = target_field
        self.relation = {
            "target_index":self.target_index,
            "base_index":self.base_index,
            "target_field":self.target_field
        }
        if 'relation.json' in os.listdir("./DATABASE/"):
            with open('./DATABASE/relation.json', "r", encoding='utf-8') as file:
                self.object = json.loads(file.read())
            for doc in self.object["relation"]:
                if doc["target_index"] == self.target_index and doc["base_index"] == self.base_index and doc[
                                                                                "target_field"] == self.target_field :
                    print("Warning : This Relation Is Indexed Before")
                    break
            else:
                self.object["relation"].append(self.relation)
                with open(f'./DATABASE/relation.json', "w") as file:
                    json.dump(self.object, file, indent=4)     
        else:
            self.object = {"relation":[self.relation]}
            with open(f'./DATABASE/relation.json', "w") as file:
                json.dump(self.object, file, indent=4)

    def open_relation(self, index_name, data):
        self.index_name = index_name
        self.data = data
        with open(f'./DATABASE/relation.json', "r", encoding='utf-8') as file:
            self.relation = json.loads(file.read())
        response = []
        for sub_data in self.data:
            for sub_relation in self.relation["relation"]:
                if sub_relation["target_index"] == self.index_name:
                    base_index = sub_relation["base_index"]
                    target_field = sub_relation["target_field"]
                    relation_field = ""
                    for sub_field in target_field.split("."):
                        relation_field += f"['{sub_field}']"
                    self.id = eval(f"{sub_data}['source']{relation_field}")[0]
                    with open(f'./DATABASE/{base_index}.json', "r", encoding='utf-8') as file:
                        self.doc_base = json.loads(file.read())
                    for doc in self.doc_base["data"]:
                        if doc["id"] == self.id:
                            exec(f"sub_data['source']{relation_field} = self.doc_base['data'][0]")
                            response.append(sub_data)
                            break
                else:
                    response = self.data
        return response

    def get_data_by_query(self, index_name, field=None, operator=None, value=None, size=None):
        response = []
        if field is not None and operator is not None and value is not None:
            if f"{index_name}.json" in os.listdir("./DATABASE/"):
                query_field = ""
                for sub_field in field.split("."):
                    query_field += f"['{sub_field}']"
                with open(f"./DATABASE/{index_name}.json", "r", encoding='utf-8') as file:
                    self.data = json.loads(file.read())
                for doc in self.data["data"]:
                    try:
                        if operator == "==":
                            if eval(f"{doc}['source']{query_field}") == value:
                                response.append(doc)
                            if len(response) == size:
                                return response
                        if operator == "!=":
                            if eval(f"{doc}['source']{query_field}") != value:
                                response.append(doc)
                            if len(response) == size:
                                return response
                        if operator == ">":
                            if eval(f"{doc}['source']{query_field}") > value:
                                response.append(doc)
                            if len(response) == size:
                                return response
                        if operator == "<":
                            if eval(f"{doc}['source']{query_field}") < value:
                                response.append(doc)
                            if len(response) == size:
                                return response
                        if operator == ">=":
                            if eval(f"{doc}['source']{query_field}") >= value:
                                response.append(doc)
                            if len(response) == size:
                                return response
                        if operator == "<=":
                            if eval(f"{doc}['source']{query_field}") <= value:
                                response.append(doc)
                            if len(response) == size:
                                return response
                    except:pass
                return response

    def sort_data(self, list, sort_field, sort_type):
        field = ""
        for sub_field in sort_field.split("."):
            field += f'["{sub_field}"]'
        if sort_type == "asc":
            list.sort(key=lambda x: eval(f'{x}["source"]{field}'))
        elif sort_type == "desc":
            list.sort(key=lambda x: eval(f'{x}["source"]{field}'))
            list = list[::-1]
        return list

    def get(self, index_name, id=None, query=None):
        self.index_name = index_name
        self.id = id
        self.query = query
        if self.id is not None and self.query is None:
            if 'relation.json' in os.listdir("./DATABASE/"):
                with open(f'./DATABASE/{self.index_name}.json', "r", encoding='utf-8') as file:
                    self.object = json.loads(file.read())
                for doc in self.object["data"]:
                    if doc["id"] == self.id:
                        return self.open_relation(index_name, [doc])[0]
        elif self.id is None and self.query is not None:
            if type(self.query) is dict:
                if 'field' in self.query:
                    self.field = self.query["field"]
                else:
                    self.field is None
                if 'operator' in self.query :
                    self.operator = self.query["operator"]
                else:
                    self.operator is None
                if 'value' in self.query:
                    self.value = self.query["value"]
                else:
                    self.value is None
                if "size" in self.query:
                    self.size = self.query["size"]
                else:
                    self.size = 10
                if "sort_field" in self.query:
                    self.sort_field = self.query["sort_field"]
                else:
                    self.sort_field is None
                if "sort_type" in self.query:
                    self.sort_type = self.query['sort_type']
                else:
                    self.sort_type = "asc"
                result = self.get_data_by_query(self.index_name, 
                                                self.field, 
                                                self.operator, 
                                                self.value, 
                                                self.size)
                if "sort_type" in self.query:
                    result = self.sort_data(result, self.sort_field, self.sort_type)
                return self.open_relation(self.index_name, result)