import typing

import sqlalchemy
import sqlalchemy.orm

import typesystem
from typesystem.schemas import SchemaMetaclass

from orm.exceptions import MultipleMatches, NoMatch
from orm.fields import ForeignKey


FILTER_OPERATORS = {
    "exact": "__eq__",
    "iexact": "ilike",
    "contains": "like",
    "icontains": "ilike",
    "in": "in_",
    "gt": "__gt__",
    "gte": "__ge__",
    "lt": "__lt__",
    "lte": "__le__",
    "search": "@@",
    "rank": "tsrank",
}


class ModelMetaclass(SchemaMetaclass):
    def __new__(
        cls: type, name: str, bases: typing.Sequence[type], attrs: dict
    ) -> type:
        new_model = super(ModelMetaclass, cls).__new__(  # type: ignore
            cls, name, bases, attrs
        )

        if attrs.get("__abstract__"):
            return new_model

        tablename = attrs["__tablename__"]
        metadata = attrs["__metadata__"]
        pkname = None

        columns = []
        for name, field in new_model.fields.items():
            if field.primary_key:
                pkname = name
            columns.append(field.get_column(name))

        if "__constraints__" in attrs:
            columns.extend(attrs["__constraints__"])

        new_model.__table__ = sqlalchemy.Table(tablename, metadata, *columns)
        new_model.__pkname__ = pkname

        return new_model


class QuerySet:
    ESCAPE_CHARACTERS = ['%', '_']
    def __init__(self, model_cls=None, filter_clauses=None, select_related=None, order_by = None, limit_count=None, offset_count=None, distinct=None):
        self.model_cls = model_cls
        self.filter_clauses = [] if filter_clauses is None else filter_clauses
        self._select_related = [] if select_related is None else select_related
        self._order_by = [] if order_by is None else order_by
        self._distinct= [] if distinct is None else distinct
        self.limit_count = limit_count
        self.offset_count = offset_count

    def __get__(self, instance, owner):
        return self.__class__(model_cls=owner)

    @property
    def database(self):
        return self.model_cls.__database__

    @property
    def table(self):
        return self.model_cls.__table__

    def build_select_expression(self):
        tables = [self.table]
        select_from = self.table

        for item in self._select_related:
            model_cls = self.model_cls
            for part in item.split("__"):
                isouter = model_cls.fields[part].allow_null
                model_cls = model_cls.fields[part].to
                select_from = sqlalchemy.sql.join(select_from, model_cls.__table__, isouter=isouter)
                tables.append(model_cls.__table__)

        expr = sqlalchemy.sql.select(tables)
        expr = expr.select_from(select_from)

        if self.filter_clauses:
            if len(self.filter_clauses) == 1:
                clause = self.filter_clauses[0]
            else:
                clause = sqlalchemy.sql.and_(*self.filter_clauses)
            expr = expr.where(clause)

        if self._order_by:
            order_args = []
            for clause in self._order_by:
                if clause.startswith("-"):
                    desc = True
                    col_name = clause.lstrip("-")
                else:
                    desc = False
                    col_name = clause

                nulls_first = False
                nulls_last = False
                if clause.endswith("__nulls_first"):
                    col_name = col_name.replace("__nulls_first", "")
                    nulls_first = True
                elif clause.endswith("__nulls_last"):
                    col_name = col_name.replace("__nulls_last", "")
                    nulls_last = True
                
                col = self.model_cls.__table__.columns.get(col_name)
                if col is not None:
                    if desc:
                        col = col.desc()
                    if nulls_first:
                        col = col.nullsfirst()
                    elif nulls_last:
                        col = col.nullslast()
                    order_args.append(col)
                else:
                    order_args.append(col_name)
            expr = expr.order_by(*order_args)

        if self._distinct:
            expr = expr.distinct(sqlalchemy.sql.and_(*([self.model_cls.__table__.columns[c] for c in self._distinct])))

        if self.limit_count:
            expr = expr.limit(self.limit_count)

        if self.offset_count:
            expr = expr.offset(self.offset_count)

        return expr

    def filter(self, or_=False, **kwargs):
        filter_clauses = self.filter_clauses
        select_related = list(self._select_related)

        clauses = []
        for key, value in kwargs.items():
            if "__" in key:
                parts = key.split("__")

                # Determine if we should treat the final part as a
                # filter operator or as a related field.
                if parts[-1] in FILTER_OPERATORS:
                    op = parts[-1]
                    field_name = parts[-2]
                    related_parts = parts[:-2]
                else:
                    op = "exact"
                    field_name = parts[-1]
                    related_parts = parts[:-1]

                model_cls = self.model_cls
                if related_parts:
                    # Add any implied select_related
                    related_str = "__".join(related_parts)
                    if related_str not in select_related:
                        select_related.append(related_str)

                    # Walk the relationships to the actual model class
                    # against which the comparison is being made.
                    for part in related_parts:
                        model_cls = model_cls.fields[part].to

                column = model_cls.__table__.columns[field_name]

            else:
                op = "exact"
                column = self.table.columns[key]

            # Map the operation code onto SQLAlchemy's ColumnElement
            # https://docs.sqlalchemy.org/en/latest/core/sqlelement.html#sqlalchemy.sql.expression.ColumnElement
            op_attr = FILTER_OPERATORS[op]
            has_escaped_character = False

            if op in ["contains", "icontains"]:
                has_escaped_character = any(c for c in self.ESCAPE_CHARACTERS
                                            if c in value)
                if has_escaped_character:
                    # enable escape modifier
                    for char in self.ESCAPE_CHARACTERS:
                        value = value.replace(char, f'\\{char}')
                value = f"%{value}%"

            if isinstance(value, Model):
                value = value.pk

            if op_attr == '@@':
                clause = column.op('@@')(sqlalchemy.func.plainto_tsquery('russian', value))
            elif op_attr == 'tsrank':
                clause = sqlalchemy.func.ts_rank(column, sqlalchemy.func.plainto_tsquery('russian', value)).label('tsrank')
            else:
                clause = getattr(column, op_attr)(value)
                clause.modifiers['escape'] = '\\' if has_escaped_character else None
            clauses.append(clause)

        if or_ is True:
            filter_clauses.append(sqlalchemy.sql.or_(*clauses))
        else:
            filter_clauses.extend(clauses)

        return self.__class__(
            model_cls=self.model_cls,
            filter_clauses=filter_clauses,
            select_related=select_related,
            order_by=self._order_by,
            distinct=self._distinct,
            limit_count=self.limit_count,
            offset_count=self.offset_count,
        )

    def select_related(self, *related):
        related = self._select_related + list(related)
        return self.__class__(
            model_cls=self.model_cls,
            filter_clauses=self.filter_clauses,
            select_related=related,
            order_by=self._order_by,
            distinct=self._distinct,
            limit_count=self.limit_count,
            offset_count=self.offset_count,
        )

    async def exists(self) -> bool:
        expr = self.build_select_expression()
        expr = sqlalchemy.exists(expr).select()
        return await self.database.fetch_val(expr)

    def order_by(self, *order_by):
        order_by = self._order_by + list(order_by)
        return self.__class__(
            model_cls=self.model_cls,
            filter_clauses=self.filter_clauses,
            select_related=self._select_related,
            order_by=order_by,
            distinct=self._distinct,
            limit_count=self.limit_count,
            offset_count=self.offset_count,
        )

    def distinct(self, *distinct):
        distinct = self._distinct + list(distinct)
        return self.__class__(
            model_cls=self.model_cls,
            filter_clauses=self.filter_clauses,
            select_related=self._select_related,
            order_by=self._order_by,
            distinct=distinct,
            limit_count=self.limit_count,
            offset_count=self.offset_count,
        )

    def limit(self, limit_count: int):
        return self.__class__(
            model_cls=self.model_cls,
            filter_clauses=self.filter_clauses,
            select_related=self._select_related,
            order_by=self._order_by,
            distinct=self._distinct,
            limit_count=limit_count,
            offset_count=self.offset_count,
        )

    def offset(self, offset_count: int):
        return self.__class__(
            model_cls=self.model_cls,
            filter_clauses=self.filter_clauses,
            select_related=self._select_related,
            order_by=self._order_by,
            distinct=self._distinct,
            limit_count=self.limit_count,
            offset_count=offset_count,
        )

    async def count(self) -> int:
        expr = self.build_select_expression()
        #expr = sqlalchemy.func.count().select().select_from(expr)
        expr = expr.with_only_columns([sqlalchemy.func.count()]).order_by(None)
        return await self.database.fetch_val(expr)

    async def only(self, *fields):
        expr = self.build_select_expression()
        columns = []
        for field in fields:
            column = getattr(self.table.c, field)
            columns.append(column)
        expr = expr.with_only_columns(columns)

        rows = await self.database.fetch_all(expr)
        return [
            self.model_cls(dict([(f, row[f]) for f in fields]))
            for row in rows
        ]


    async def all(self, **kwargs):
        if kwargs:
            return await self.filter(**kwargs).all()

        expr = self.build_select_expression()
        rows = await self.database.fetch_all(expr)
        return [
            self.model_cls.from_row(row, select_related=self._select_related)
            for row in rows
        ]

    async def get(self, **kwargs):
        if kwargs:
            return await self.filter(**kwargs).get()

        expr = self.build_select_expression().limit(2)
        rows = await self.database.fetch_all(expr)

        if not rows:
            raise NoMatch()
        if len(rows) > 1:
            raise MultipleMatches()
        return self.model_cls.from_row(rows[0], select_related=self._select_related)

    async def create(self, **kwargs):
        # Validate the keyword arguments.
        fields = self.model_cls.fields
        required = [key for key, value in fields.items() if not value.has_default()]
        validator = typesystem.Object(
            properties=fields, required=required, additional_properties=False
        )
        kwargs = validator.validate(kwargs)

        # Remove primary key when None to prevent not null constraint in postgresql.
        pkname = self.model_cls.__pkname__
        pk = self.model_cls.fields[pkname]
        if kwargs[pkname] is None and pk.allow_null:
            del kwargs[pkname]

        # Build the insert expression.
        expr = self.table.insert()
        expr = expr.values(**kwargs)

        # Execute the insert, and return a new model instance.
        instance = self.model_cls(kwargs)
        instance.pk = await self.database.execute(expr)
        return instance


class Model(typesystem.Schema, metaclass=ModelMetaclass):
    __abstract__ = True

    objects = QuerySet()

    def __init__(self, *args, **kwargs):
        if "pk" in kwargs:
            kwargs[self.__pkname__] = kwargs.pop("pk")
        super().__init__(*args, **kwargs)

    @property
    def pk(self):
        return getattr(self, self.__pkname__)

    @pk.setter
    def pk(self, value):
        setattr(self, self.__pkname__, value)

    async def update(self, **kwargs):
        # Validate the keyword arguments.
        fields = {key: field for key, field in self.fields.items() if key in kwargs}
        validator = typesystem.Object(properties=fields)
        kwargs = validator.validate(kwargs)

        # Build the update expression.
        pk_column = getattr(self.__table__.c, self.__pkname__)
        expr = self.__table__.update()
        expr = expr.values(**kwargs).where(pk_column == self.pk)

        # Perform the update.
        await self.__database__.execute(expr)

        # Update the model instance.
        for key, value in kwargs.items():
            setattr(self, key, value)

    async def delete(self):
        # Build the delete expression.
        pk_column = getattr(self.__table__.c, self.__pkname__)
        expr = self.__table__.delete().where(pk_column == self.pk)

        # Perform the delete.
        await self.__database__.execute(expr)

    async def load(self):
        # Build the select expression.
        pk_column = getattr(self.__table__.c, self.__pkname__)
        expr = self.__table__.select().where(pk_column == self.pk)

        # Perform the fetch.
        row = await self.__database__.fetch_one(expr)

        # Update the instance.
        for key, value in dict(row).items():
            setattr(self, key, value)

    @classmethod
    def from_row(cls, row, select_related=[]):
        """
        Instantiate a model instance, given a database row.
        """
        item = {}

        # Instantiate any child instances first.
        for related in select_related:
            if "__" in related:
                first_part, remainder = related.split("__", 1)
                model_cls = cls.fields[first_part].to
                item[first_part] = model_cls.from_row(row, select_related=[remainder])
            else:
                model_cls = cls.fields[related].to
                item[related] = model_cls.from_row(row)

        # Pull out the regular column values.
        for column in cls.__table__.columns:
            if column.name not in item:
                item[column.name] = row[column]

        return cls(item)

    def __setattr__(self, key, value):
        if key in self.fields:
            # Setting a relationship to a raw pk value should set a
            # fully-fledged relationship instance, with just the pk loaded.
            value = self.fields[key].expand_relationship(value)
        super().__setattr__(key, value)
