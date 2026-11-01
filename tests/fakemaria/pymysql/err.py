"""pymysql's exception hierarchy, by the same names. Each is raised with
(errno, message), as pymysql does, so str(e) reads the same."""


class MySQLError(Exception):
    pass


class Warning(Warning, MySQLError):      # noqa: A001 — pymysql's own name
    pass


class Error(MySQLError):
    pass


class InterfaceError(Error):
    pass


class DatabaseError(Error):
    pass


class DataError(DatabaseError):
    pass


class OperationalError(DatabaseError):
    pass


class IntegrityError(DatabaseError):
    pass


class InternalError(DatabaseError):
    pass


class ProgrammingError(DatabaseError):
    pass


class NotSupportedError(DatabaseError):
    pass
