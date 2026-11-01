"""Cursor (rows as tuples) and DictCursor (rows as dicts), as pymysql has them."""
from . import err


class Cursor:
    max_stmt_length = 1024000

    def __init__(self, connection):
        self.connection = connection
        self.description = None
        self.rowcount = -1
        self.lastrowid = None
        self.arraysize = 1
        self._rows: list = []
        self._i = 0

    # -- running -----------------------------------------------------------
    def execute(self, query, args=None) -> int:
        self._check()
        self.connection._run(self, query, args)
        return self.rowcount

    def executemany(self, query, args) -> int:
        self._check()
        if not args:
            return 0
        self.connection._run_many(self, query, list(args))
        return self.rowcount

    def _check(self):
        if self.connection is None:
            raise err.ProgrammingError("Cursor closed")

    def _set(self, names, rows, rowcount, lastrowid):
        if names is None:
            self.description = None
            self._rows = []
        else:
            self.description = tuple((n, None, None, None, None, None, True)
                                     for n in names)
            self._rows = [self._row(names, r) for r in rows]
        self._i = 0
        self.rowcount = rowcount
        self.lastrowid = lastrowid

    def _row(self, names, values):
        return tuple(values)

    # -- reading -----------------------------------------------------------
    def fetchone(self):
        if self._i >= len(self._rows):
            return None
        r = self._rows[self._i]
        self._i += 1
        return r

    def fetchmany(self, size=None):
        n = size or self.arraysize
        out = self._rows[self._i:self._i + n]
        self._i += len(out)
        return out

    def fetchall(self):
        out = self._rows[self._i:]
        self._i = len(self._rows)
        return out

    def __iter__(self):
        return iter(self.fetchone, None)

    def close(self):
        self.connection = None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
        return False


class DictCursor(Cursor):
    def _row(self, names, values):
        # pymysql: the first column of a name keeps it; a later one is
        # keyed "table.name". sqlite does not say which table a result
        # column came from, so a later duplicate is keyed "?.name".
        out: dict = {}
        for n, v in zip(names, values):
            out[n if n not in out else "?." + n] = v
        return out


SSCursor = Cursor
SSDictCursor = DictCursor
