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
    __insert_sibling_query = 'INSERT INTO "{table}" ({columns}) VALUES {multiple_placeholders}'
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
        self.__children = {}
        self.__siblings = {}

    def __getattr__(self, name):
        # check, if instance is modified and throw an exception
        # get corresponding data from database if needed
        # check, if requested property name is in current class
        #    columns, parents, children or siblings and call corresponding
        #    getter with name as an argument
        # throw an exception, if attribute is unrecognized
        if self.__modified:
            raise ModifiedError

        if name in self._columns:
            self.__load()
            return self._get_column(name)
        elif name in self._parents:
            self.__load()
            return self._get_parent(name)
        elif name in self._children:
            self.__load()
            return self._get_children(name)
        elif name in self._siblings:
            self.__load()
            return self._get_siblings(name)
        else:
            raise AttributeError

    def __setattr__(self, name, value):
        # check, if requested property name is in current class
        #    columns, parents, children or siblings and call corresponding
        #    setter with name and value as arguments or use default implementation
        if name in self._parents:
            self._set_parent(name, value)
            self.__modified = True
        elif name in self._columns:
            self._set_column(name, value)
            self.__modified = True
        elif name in self._siblings:
            self._set_siblings(name, value)
            self.__modified = True
        elif name in self._children:
            self._set_children(name, value)
            self.__modified = True
        else:
            super(Entity, self).__setattr__(name, value)

    def __execute_query(self, query, args):
        print(f'{query}    {args}')
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
        columns = ", ".join(self.__fields.keys())
        placeholders = ", ".join(["%s" for _ in range(len(self.__fields.keys()))])

        query = self.__insert_query.format(table=self.__table, columns=columns,
                                           placeholders=placeholders)

        self.__execute_query(query, tuple(self.__fields.values()))
        self.__id = self.__cursor.fetchone()[0]

    def __load(self):
        # if current instance is not loaded yet — execute select statement and store it's result as an associative array
        # (fields), where column names used as keys
        if not self.__loaded:
            query = self.__select_query.format(table=self.__table)
            self.__execute_query(query, (self.__id,))
            data = self.__cursor.fetchone()
            if not data:
                raise NotFoundError

            self.__fields.update(data)
            self.__loaded = True

    def __update(self):
        # generate an update query string from fields keys and values and execute it
        # use prepared statements

        columns = []
        args = []
        for k, v in self.__fields.items():
            columns.append(f"{k}=%s")
            args.append(v)

        columns = ", ".join(columns)
        args.append(self.__id)

        query = self.__update_query.format(table=self.__table, columns=columns)
        self.__execute_query(query, tuple(args))

    def __update_chldr(self):
        # __update_children = 'UPDATE "{table}" SET {parent}_id=%s WHERE {table}_id IN ({children})'
        for child, ids in self.__children.items():
            query = self.__update_children.format(table=child, parent=self.__table,
                                                  children=", ".join([str(x) for x in ids]))
            self.__execute_query(query, (self.__id,))

    def __insert_sbl(self):
        # __insert_sibling_query = 'INSERT INTO "{table}" ({columns}) VALUES {multiple_placeholders}'

        for assigned_table, val in self.__siblings.items():
            col = [f'{self.__table}_id', f'{assigned_table}_id']
            columns = ", ".join(col)
            placeholders = ", ".join(["%s" for _ in range(len(col))])

            multiple_rows = ", ".join('({placeholders})' for _ in range(len(val))).format(placeholders=placeholders)
            pivot_name = '__'.join(sorted([assigned_table, self.__table]))

            query = self.__insert_sibling_query.format(table=pivot_name,
                                                       columns=columns,
                                                       multiple_placeholders=multiple_rows)
            list_ids = []
            for upd_id in val:
                list_ids.append(self.__id)
                list_ids.append(upd_id)

            self.__execute_query(query, tuple(list_ids,))

    def _get_children(self, name):
        # return an array of child entity instances
        # each child instance must have an id and be filled with data
        # __parent_query = 'SELECT * FROM "{table}" WHERE {parent}_id=%s'

        array_instances = []
        mod = __import__('models')
        my_cls = getattr(mod, self._children[name])
        children_table = self._children[name].lower()

        query = self.__parent_query.format(table=children_table, parent=self.__table)
        self.__execute_query(query, (self.__id,))
        list_data = self.__cursor.fetchall()

        if not list_data:
            raise NotFoundError
        for data in list_data:
            row_id = data[f'{children_table}_id']
            my_instance = my_cls(row_id)
            array_instances.append(my_instance)

        return array_instances

    def _get_column(self, name):
        # return value from fields array by <table>_<name> as a key
        return self.__fields[f'{self.__table}_{name}']

    def _get_parent(self, name):
        # ORM part 2
        # get parent id from fields with <name>_id as a key
        # return an instance of parent entity class with an appropriate id
        mod = __import__('models')
        my_cls = getattr(mod, name.capitalize())
        my_instance = my_cls(self.__fields[f'{name}_id'])

        return my_instance

    def _get_siblings(self, name):
        # ORM part 2
        # get parent id from fields with <name>_id as a key
        # return an array of sibling entity instances
        # each sibling instance must have an id and be filled with data
        # __sibling_query = 'SELECT * FROM "{sibling}" NATURAL JOIN "{join_table}" WHERE {table}_id=%s'

        array_instances = []
        mod = __import__('models')
        my_cls = getattr(mod, self._siblings[name])
        sibling_table = self._siblings[name].lower()

        query = self.__sibling_query.format(sibling=sibling_table,
                                            join_table=f'{self.__table}__{sibling_table}',
                                            table=self.__table)

        self.__execute_query(query, (self.__id,))
        list_data = self.__cursor.fetchall()

        if not list_data:
            raise NotFoundError
        for data in list_data:
            row_id = data[f'{sibling_table}_id']
            my_instance = my_cls(row_id)
            my_instance.__fields = data
            my_instance.__loaded = True
            array_instances.append(my_instance)

        return array_instances

    def _set_siblings(self, name, value):

        sibling_table = self._siblings[name].lower()
        sbl = []
        for v_id in value:
            sbl.append(v_id.id)
        self.__siblings[sibling_table] = sbl

    def _set_column(self, name, value):
        # put new value into fields array with <table>_<name> as a key
        self.__fields[f'{self.__table}_{name}'] = value

    def _set_parent(self, name, value):
        # ORM part 2
        # put new value into fields array with <name>_id as a key
        # value can be a number or an instance of Entity subclass
        self.__fields[f'{name}_id'] = value.id

    def _set_children(self, name, value):
        # __update_children = 'UPDATE "{table}" SET {parent}_id=%s WHERE {table}_id IN ({children})'
        chl = []
        for cat_id in value:
            chl.append(cat_id.id)
        child_table = self._children[name].lower()
        self.__children[child_table] = chl

    @classmethod
    def all(cls):
        # get ALL rows with ALL columns from corresponding table
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
        if self.__fields:
            if not self.__id:
                self.__insert()
                self.__load()
            else:
                self.__update()
        if self.__children:
            self.__update_chldr()
        if self.__siblings:
            self.__insert_sbl()
