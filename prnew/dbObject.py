from datetime import date, datetime

from typing_extensions import Self

from ...utils.dateAndTime import toIsoFormat
from .baseDbObject import BaseDbObject


class DbObject(BaseDbObject):
    def __repr__(self):
        def _format(val):
            r = getattr(self, val)

            r = toIsoFormat(r, passThrough0nWrongType=True)
            if type(r) in (date, datetime):
                return toIsoFormat(r)

            elif isinstance(r, DbObject):
                return r.serialisedName()  # Assuming it exists
            else:
                return r

        classname = f"{self.__class__.__name__}"
        fields = ", ".join([f"{f}={_format(f)}" for f in self.dict().keys()])
        return f"{classname}({fields})"

    def serialisedName(self):
        return str(self)

    @classmethod
    def byName(cls, name: str) -> Self:
        return cls.getOneWhere("name=%s", [name.strip()])

    @classmethod
    def byDescription(cls, description: str) -> Self:
        return cls.getOneWhere("description=%s", [description.strip()])

    @classmethod
    def load(cls, objectId: str = None) -> Self:
        if objectId is not None:
            entity = cls.getOne(objectId)
            return entity
        else:
            entities = cls.getAll()
            return entities

    def _trySerialise(self, value, deepDive: bool) -> str | int | float | dict:
        # Special date and datetime treatment
        if type(value) in (date, datetime):
            return toIsoFormat(value)

        elif isinstance(value, DbObject):
            if deepDive:
                return value.serialise(deepDive=deepDive)
            else:
                return value.serialisedName()  # Assuming it exists
        else:
            return value

    def __attributes(self):
        return list(self.dict().keys()) + [
            p
            for p in vars(self.__class__)
            if type(getattr(self.__class__, p)) == property
        ]

    def _serialiseDb(self) -> dict:
        # It will be serialised even if it is disabled/deleted.
        # Second param must be includeDisabled flag
        data = self.db.callSP(self.__class__.serialiseAllSP(), [self.id, True])
        if data:
            return data[0]
        else:
            return {}

    def serialise(self, useStoredProcedure: bool = True, deepDive: bool = True) -> dict:
        if useStoredProcedure and self.__class__.serialiseAllSP():
            return self._serialiseDb()
        else:
            return self.dict()

    @classmethod
    def serialiseAllSP(cls) -> str:
        return None

    @classmethod
    def serialiseAll(
        cls,
        entities: list = None,
        useStoredProcedure: bool = True,
        deepDive: bool = True,
    ) -> list[dict]:
        if useStoredProcedure and cls.serialiseAllSP():
            # It will be serialised even if it is disabled/deleted.
            # Second param must be includeDisabled flag
            data = cls.db.callSP(cls.serialiseAllSP(), [None, True])
            return data
        else:
            if entities is None:
                entities = cls.load()

            return [
                e.serialise(useStoredProcedure=False, deepDive=deepDive)
                for e in entities
            ]

    def save(self, reload=False):
        """It will save the existing values to the DB regardless if they are new,
        dirty or not. Also, reloads itself from the DB if requested
        """
        values = self.dict()
        if self.id is None:
            new = self.insert(self, returnNew=reload)
        else:
            new = self.update(values, returnNew=reload)

        if new is not None:
            self.__dict__ |= new.dict()

    @classmethod
    def searchOne(cls, **kwargs) -> Self:  # noqa
        """
        Search for one and only one object of the class based on the provided
        filters as part of **kwargs dict **kwargs must match the
        class's properties/fields in the DB
        TODO: allow linked properties like service.name
        """
        whereClause = " AND ".join([f"{k}=%s" for k in kwargs.keys()])
        params = [v for v in kwargs.values()]
        return cls.getOneWhere(whereClause, params)

    @classmethod
    def searchAll(cls, orderClause: str = None, **kwargs) -> list[Self]:  # noqa
        whereClause = " AND ".join([f"{k}=%s" for k in kwargs.keys()])
        params = [v for v in kwargs.values()]
        if orderClause:
            if whereClause:
                sqlQuery = f"SELECT * FROM {cls.qualifiedName} WHERE {whereClause} ORDER BY {orderClause}"
            else:
                sqlQuery = f"SELECT * FROM {cls.qualifiedName} ORDER BY {orderClause}"
            return cls.getAll(sqlQuery=sqlQuery, params=params)
        else:
            if whereClause:
                return cls.getAll(whereClause=whereClause, params=params)
            else:
                return cls.getAll()