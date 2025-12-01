import os

from psycopg2.extras import RealDictCursor
from typing_extensions import Self

from ...pg.database.exceptions import DoesNotExistException, NothingToUpdateException
from ...utilities import classproperty
from ..database.exceptions import MoreThanOneException

try:
    from ...constants import DB_ENVIRONMENT  # @UnresolvedImport
except ModuleNotFoundError:
    from client_code.constants import DB_ENVIRONMENT


class BaseDbObject(object):
    __tablename__ = ""
    db = None

    @classproperty
    def schemaSuffix(self):
        return ""

    @classproperty
    def schema(cls):  # @NoSelf
        default = DB_ENVIRONMENT

        res = os.environ.get(
            "PG_SCHEMA_NAME", os.environ.get("PG_SCHEMA_NAME_TESTS", default).lower()
        )
        return f"{res}{cls.schemaSuffix}"

    @classproperty
    def qualifiedName(cls):  # @NoSelf
        return f"{cls.schema}.{cls.__tablename__}"

    def dict(self):
        d = {k: v for k, v in self.__dict__.items() if not k.startswith("_")}
        return d

    def __eq__(self, other):
        if type(self) != type(other):
            return False
        return self.__dict__ == other.__dict__

    @classmethod
    def getOne(cls, _id: int) -> Self:
        return cls.getOneWhere(f"id = {_id}")

    @classmethod
    def getOneWhere(cls, whereClause: str, params=None) -> Self:
        with cls.db.connection as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                sql = f"SELECT * FROM {cls.qualifiedName} WHERE {whereClause}"
                cursor.execute(sql, params)
                res = cursor.fetchall()

                if len(res) < 1:
                    query = f"QUERY: {sql}. Params: {params}"
                    msg = f"Expected 1 result, got nothing. For query: {query}"
                    raise DoesNotExistException(msg)
                elif len(res) > 1:
                    query = f"QUERY: {sql}. Params: {params}"
                    msg = f"Expected 1 result only, got {len(res)}. For query: {query}"
                    raise MoreThanOneException(msg)
                else:
                    return cls(**dict(res[0]))

    @classmethod
    def getAll(
        cls,
        whereClause: str = None,
        sqlQuery: str = None,
        params=None
    ) -> list[Self]:
        assert (
            whereClause is None
            and sqlQuery is None
            or (whereClause or "" != sqlQuery or "")
        ), "whereClause and sqlQuery cannot be supplied at the same time"

        with cls.db.connection as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                if sqlQuery:
                    cursor.execute(sqlQuery, params)
                else:
                    if whereClause:
                        cursor.execute(
                            f"SELECT * FROM {cls.qualifiedName} WHERE {whereClause}",
                            params,
                        )
                    else:
                        cursor.execute(f"SELECT * FROM {cls.qualifiedName}", params)
                res = cursor.fetchall()

        if res:
            return [cls(**dict(obj)) for obj in res]
        else:
            return []

    def update(self, values: dict, returnNew=False) -> Self | None:
        assert self.id is not None

        self.updateWhere(values, f"id = {self.id}")
        if returnNew:
            return self.getOne(self.id)

    @classmethod
    def updateWhere(cls, values: dict, whereClause: str) -> None:
        # TODO: use execute_values for multiple updates
        # from psycopg2.extras import execute_values
        assert whereClause

        setStm = ", ".join([f"{key}=%s" for key in values.keys()])
        params = list(values.values())

        if not setStm or not params:
            raise NothingToUpdateException()

        if whereClause:
            sql = f"UPDATE {cls.qualifiedName} SET {setStm} WHERE {whereClause}"
        else:
            sql = f"UPDATE {cls.qualifiedName} SET {setStm}"

        with cls.db.connection as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, params)

    @classmethod
    def countWhere(cls, whereClause: str = None) -> int:
        with cls.db.connection as conn:
            with conn.cursor() as cursor:
                if whereClause:
                    cursor.execute(
                        f"SELECT COUNT(1) FROM {cls.qualifiedName} WHERE {whereClause}"
                    )
                else:
                    cursor.execute(f"SELECT COUNT(1) FROM {cls.qualifiedName}")
                count = cursor.fetchone()
                return count[0]

    @classmethod
    def insert(cls, obj: Self, returnNew=False) -> Self:
        values = {
            key: value for key, value in obj.__dict__.items() if value is not None
        }
        cols = [f'"{col}"' for col in values.keys()]
        cols = ", ".join(cols)

        params = list(values.values())
        values = ", ".join(["%s" for value in values.keys()])

        with cls.db.connection as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    f"INSERT INTO {cls.qualifiedName}({cols}) VALUES({values}) RETURNING id",
                    params,
                )
                if returnNew:
                    objId = cursor.fetchone()[0]
                    return cls.getOne(objId)

    @classmethod
    def deleteWhere(cls, whereClause: str) -> None:
        with cls.db.connection as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"DELETE FROM {cls.qualifiedName} WHERE {whereClause}")

    def delete(self) -> Self | None:
        assert self.id is not None
        self.deleteWhere(f"id = {self.id}")

    @classmethod
    def query(cls, sqlQuery: str, params: list = None) -> list:
        with cls.db.connection as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(sqlQuery, params)
                res = cursor.fetchall()
                return res