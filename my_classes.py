import yaml


class SelfRelationException(Exception):
    pass


class PairRelationException(Exception):
    pass


class ValueRelationException(Exception):
    pass


def parse_yaml_schema(yaml_schema_path: str) -> dict:
    with open(yaml_schema_path) as file:
        return yaml.safe_load(file)


def write_to_file(sql_schema_path: str, queries: list):
    with open(sql_schema_path, 'a', encoding='utf8') as file:
        for query in queries:
            file.write(query + "\n \n")


class Generator:
    """Generator create sql statement from yaml schema and create triggers for tables"""

    def __init__(self, raw_dict):
        self.__queries = []
        self.__raw_dict = raw_dict

    def create_sql_queries(self):

        for table in self.__raw_dict:
            table_name_query = f'CREATE TABLE "{table.lower()}"'
            body_query = []
            body_query.append(f'\n    "{table.lower()}_id" SERIAL PRIMARY KEY')
            for column_name, column_type in self.__raw_dict[table]['fields'].items():
                body_query.append(f'\n    "{table.lower()}_{column_name}" {column_type}')
            body_query.append(f'\n    "{table.lower()}_created" '
                              f'INTEGER NOT NULL DEFAULT cast(extract(epoch from now()) AS INTEGER)')
            body_query.append(f'\n    "{table.lower()}_updated" '
                              f'INTEGER NOT NULL DEFAULT cast(extract(epoch from now()) AS INTEGER)')
            self.__queries.append(f"{table_name_query} ({','.join(body_query)}\n);")

    def create_triggers(self):
        for table in self.__raw_dict:
            trigger_body = f'CREATE OR REPLACE FUNCTION update_{table.lower()}_timestamp()\n' \
                           f'RETURNS TRIGGER AS $$\n' \
                           f'BEGIN\n' \
                           f'    NEW.{table.lower()}_updated = cast(extract(epoch from now()) as integer);\n' \
                           f'    RETURN NEW;\n' \
                           f'END;\n' \
                           f'$$ language "plpgsql";\n' \
                           f'CREATE TRIGGER "tr_{table.lower()}_updated" BEFORE UPDATE ON "{table.lower()}" ' \
                           f'FOR EACH ROW EXECUTE PROCEDURE update_{table.lower()}_timestamp();'
            self.__queries.append(trigger_body)

    def validate_schema(self):
        one = 'one'
        many = 'many'
        relation_values = (one, many)

        for table in self.__raw_dict:
            for relation_table, relation_value in self.__raw_dict[table]['relations'].items():
                if relation_table == table:
                    raise SelfRelationException(f"Duplicate table: {relation_table} and {table}")
                if relation_value not in relation_values:
                    raise ValueRelationException(f"Relation value: '{relation_value}' isn't correct")

    def one_to_many(self):
        for table in self.__raw_dict:
            for relation_table, relation_value in self.__raw_dict[table]['relations'].items():

                if relation_value == 'one' and self.__raw_dict[relation_table]['relations'][table] == 'many':
                    one_to_many = f'ALTER TABLE "{table.lower()}" ADD "{relation_table.lower()}_id" INTEGER NOT NULL, ' \
                                    f'\n    ADD CONSTRAINT "fk_{table.lower()}_{relation_table.lower()}_id" ' \
                                    f'FOREIGN KEY ("{relation_table.lower()}_id") ' \
                                    f'REFERENCES "{relation_table.lower()}" ("{relation_table.lower()}_id");'
                    self.__queries.append(one_to_many)

    def many_to_many(self):
        for table in self.__raw_dict:
            for relation_table, relation_value in self.__raw_dict[table]['relations'].items():

                if relation_value == 'many' and self.__raw_dict[relation_table]['relations'][table] == 'many':

                    many_to_many = f'ALTER TABLE "{table.lower()}__{relation_table.lower()}"\n    ' \
                                    f'ADD CONSTRAINT "fk_{table.lower()}__{relation_table.lower()}_{table.lower()}_id" ' \
                                    f'FOREIGN KEY ("{table.lower()}_id")' \
                                    f' REFERENCES "{table.lower()}" ("{table.lower()}_id");'
                    self.__queries.append(many_to_many)

    def create_pivot_table(self):
        handled_tables_name = set()

        for table in self.__raw_dict:
            for relation_table, relation_value in self.__raw_dict[table]['relations'].items():

                if relation_value == 'many' and self.__raw_dict[relation_table]['relations'][table] == 'many':
                    pivot_name = '__'.join(sorted([table.lower(), relation_table.lower()]))
                    if pivot_name in handled_tables_name:
                        continue

                    table_query = f'CREATE TABLE "{pivot_name}" (' \
                                    f'\n    "{table.lower()}_id" INTEGER NOT NULL,' \
                                    f'\n    "{relation_table.lower()}_id" INTEGER NOT NULL,' \
                                    f'\n    PRIMARY KEY ("{table.lower()}_id", "{relation_table.lower()}_id")\n);'

                    handled_tables_name.add(f"{pivot_name}")
                    self.__queries.append(table_query)

    def change_table(self):
        self.create_pivot_table()
        self.one_to_many()
        self.many_to_many()

    def generate_scheme(self):
        self.validate_schema()
        self.create_sql_queries()
        self.change_table()
        self.create_triggers()
        return self.__queries


schema_task_three = 'schema_task_3.yml'
sql_file_task_three = 'sql_schema_3.sql'

if __name__ == "__main__":
    generator = Generator(parse_yaml_schema(schema_task_three))

    schema = generator.generate_scheme()
    write_to_file(sql_file_task_three, schema)

