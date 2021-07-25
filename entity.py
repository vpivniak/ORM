import psycopg2
from psycopg2.extras import DictCursor


class DatabaseError(Exception):
    pass


class NotFoundError(Exception):
    pass


class ModifiedError(Exception):
    pass


class RuntimeException(Exception):
    pass


class Entity(object):
    db = None

    # ORM part 1
    __delete_query = 'DELETE FROM "{table}" WHERE {table}_id=%s'
    __insert_query = 'INSERT INTO "{table}" ({columns}) VALUES ({placeholders}) RETURNING "{table}_id"'
    __list_query = 'SELECT * FROM "{table}"'
    __select_query = 'SELECT * FROM "{table}" WHERE {table}_id=%s'
    __update_query = 'UPDATE "{table}" SET {columns} WHERE {table}_id=%s'

    # ORM part 2
    __parent_query = 'SELECT * FROM "{table}" WHERE {parent}_id=%s'
    __sibling_query = 'SELECT * FROM "{sibling}" NATURAL JOIN "{join_table}" WHERE {table}_id=%s'
    __update_children = 'UPDATE "{table}" SET {parent}_id=%s WHERE {table}_id IN ({children})'

    def __init__(self, id=None):
        if self.__class__.db is None:
            raise DatabaseError()

        self.__cursor = self.__class__.db.cursor(
            cursor_factory=psycopg2.extras.DictCursor
        )
        self.__fields = {}
        self.__id = id
        self.__loaded = False
        self.__modified = False
        self.__table = self.__class__.__name__.lower()

    def __getattr__(self, name):
        # check, if instance is modified and throw an exception
        # get corresponding data from database if needed
        # check, if requested property name is in current class
        #    columns, parents, children or siblings and call corresponding
        #    getter with name as an argument
        # throw an exception, if attribute is unrecognized
        # atr = []
        # for k, v in self.__class__.__dict__.items():
        #     if not k.startswith('__'):
        #         for c in v:
        #             atr.append(c)
        # if name not in atr:
        if name not in self._columns:
            raise AttributeError
        if self.__modified:
            raise ModifiedError
        self.__load()
        return self._get_column(name)

    def __setattr__(self, name, value):
        # check, if requested property name is in current class
        #    columns, parents, children or siblings and call corresponding
        #    setter with name and value as arguments or use default implementation

        if name in self._columns:
            self._set_column(name, value)
            self.__modified = True
        else:
            super(Entity, self).__setattr__(name, value)

    def __execute_query(self, query, args):
        # execute an sql statement and handle exceptions together with transactions
        try:
            self.__cursor.execute(query, args)
            self.db.commit()
            self.__modified = False
        except Exception as e:
            print(e)
            self.db.rollback()
            raise DatabaseError

    def __insert(self):
        # generate an insert query string from fields keys and values and execute it
        # use prepared statements
        # save an insert id
        # __insert_query = 'INSERT INTO "{table}" ({columns}) VALUES ({placeholders}) RETURNING "{table}_id"'
        columns = ", ".join(self.__fields.keys())
        placeholders = ", ".join(["%s" for _ in range(len(self.__fields.keys()))])
        # print(columns)
        # print(placeholders)

        query = self.__insert_query.format(table=self.__table, columns=columns,
                                           placeholders=placeholders)
        print(query)
        self.__execute_query(query, tuple(self.__fields.values()))
        self.__id = self.__cursor.fetchone()[0]

    def __load(self):
        # if current instance is not loaded yet — execute select statement and store it's result as an associative array
        # (fields), where column names used as keys
        # __select_query = 'SELECT * FROM "{table}" WHERE {table}_id=%s'
        if not self.__loaded:
            query = self.__select_query.format(table=self.__table)
            print(query)
            self.__execute_query(query, (self.__id,))
            data = self.__cursor.fetchone()
            if not data:
                raise NotFoundError

            self.__fields.update(data)
            self.__loaded = True

    def __update(self):
        # generate an update query string from fields keys and values and execute it
        # use prepared statements
        # __update_query = 'UPDATE "{table}" SET {columns} WHERE {table}_id=%s'
        columns = []
        args = []
        for k, v in self.__fields.items():
            columns.append(f"{k}=%s")
            args.append(v)
        # print(columns)
        # print(args)
        columns = ", ".join(columns)
        args.append(self.__id)
        # print(columns)
        # print(args)
        query = self.__update_query.format(table=self.__table, columns=columns)
        print(query)
        self.__execute_query(query, tuple(args))

    def _get_children(self, name):
        # return an array of child entity instances
        # each child instance must have an id and be filled with data
        pass

    def _get_column(self, name: str) -> str:
        # return value from fields array by <table>_<name> as a key
        return self.__fields[f'{self.__table}_{name}']

    def _get_parent(self, name):
        # ORM part 2
        # get parent id from fields with <name>_id as a key
        # return an instance of parent entity class with an appropriate id
        __parent_query = 'SELECT * FROM "{table}" WHERE {parent}_id=%s'
        query = self.__parent_query.format(table=self.__table, parent=self._parents[name])

        pass

    def _get_siblings(self, name):
        # ORM part 2
        # get parent id from fields with <name>_id as a key
        # return an array of sibling entity instances
        # each sibling instance must have an id and be filled with data
        pass

    def _set_column(self, name, value):
        # put new value into fields array with <table>_<name> as a key
        self.__fields[f'{self.__table}_{name}'] = value

    def _set_parent(self, name, value):
        # ORM part 2
        # put new value into fields array with <name>_id as a key
        # value can be a number or an instance of Entity subclass
        pass

    @classmethod
    def all(cls):
        # get ALL rows with ALL columns from corrensponding table
        # for each row create an instance of appropriate class
        # each instance must be filled with column data, a correct id and MUST NOT query a database for own fields any more
        # return an array of instances
        table_name = cls.__name__.lower()
        data = []

        # cursor = cls.db.cursor(
        #     cursor_factory=psycopg2.extras.DictCursor
        # )
        # try:
        #     cursor.execute(cls.__list_query.format(table=table_name), tuple())
        #     res = cursor.fetchall()
        #     cls.db.commit()
        # except Exception as e:
        #     print(e)
        #     cls.db.rollback()
        #     raise DatabaseError

        tmp_obj = cls()
        tmp_obj.__execute_query(cls.__list_query.format(table=table_name), tuple())
        res = tmp_obj.__cursor.fetchall()

        for el in res:
            obj = cls()
            obj.__fields = el
            obj.__id = el.get(f"{table_name}_id")
            obj.__loaded = True
            data.append(obj)
        return data

    def delete(self):
        # execute delete query with appropriate id
        if not self.__id:
            raise RuntimeException
        query = self.__delete_query.format(table=self.__table)
        self.__execute_query(query, (self.__id,))

    @property
    def id(self):
        # try to guess yourself
        return self.__id

    @property
    def created(self):
        # try to guess yourself
        return self.__fields[f'{self.__table}_created']

    @property
    def updated(self):
        # try to guess yourself
        return self.__fields[f'{self.__table}_updated']

    def save(self):
        # execute either insert or update query, depending on instance id
        if not self.__id:
            self.__insert()
            self.__load()
        else:
            self.__update()
