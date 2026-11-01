"""MariaDB SQL in, sqlite SQL out — for the statements the broker sends.

One rule above all: a statement this cannot translate FAITHFULLY raises
Untranslatable, which the connection turns into
ProgrammingError("fakemaria cannot run: …"). A gap must be loud. The cases
that would otherwise be silent — sqlite accepting the text and meaning
something else by it — are rewritten or refused here, each named below.

The work happens on a MASKED copy of the statement: comments removed, every
string literal replaced by \\x02n\\x02 and every parameter by \\x01n\\x01. So no
rewrite below can touch the inside of a string, and a rewrite may move or
copy a fragment freely — its parameters travel with it, because they are
bound by index only at the very end (finish()).
"""
from __future__ import annotations

import re

MARK = "\x01"     # \x01<n>\x01  = parameter n
LIT = "\x02"      # \x02<n>\x02  = string literal n
IDENT = r'(?:`[^`]+`|[A-Za-z_][A-Za-z0-9_$]*)'

# Byte / character ceilings MariaDB enforces in strict mode (its default,
# and nothing in a2a_mcp/my.cnf changes sql_mode).
TEXT_BYTES = {"TINYTEXT": 255, "TEXT": 65535, "MEDIUMTEXT": 16777215,
              "LONGTEXT": None}
INT_RANGE = {"TINYINT": (-128, 127), "SMALLINT": (-32768, 32767),
             "MEDIUMINT": (-8388608, 8388607),
             "INT": (-2147483648, 2147483647),
             "INTEGER": (-2147483648, 2147483647), "BIGINT": None}
REAL_TYPES = {"DOUBLE", "FLOAT", "REAL", "DECIMAL", "NUMERIC"}
BLOB_TYPES = {"BLOB", "TINYBLOB", "MEDIUMBLOB", "LONGBLOB", "VARBINARY",
              "BINARY"}

# Functions sqlite lacks or names differently. Rewritten to fm_<name>,
# which the connection registers, so their MariaDB meaning holds.
FUNCS = ("VERSION", "DATABASE", "SCHEMA", "CURRENT_USER", "NOW",
         "UNIX_TIMESTAMP", "CONCAT", "GREATEST", "LEAST")


class Untranslatable(Exception):
    pass


# --------------------------------------------------------------------------
# Step 1 — what pymysql itself does first: `%` formatting of the WHOLE string.

def bind_markers(sql: str, args) -> tuple[str, list]:
    """`%s` -> a parameter marker, `%%` -> `%`, exactly when pymysql formats:
    whenever args is not None (an empty tuple included). A sequence argument
    becomes a parenthesised list, as pymysql escapes it."""
    if args is None:
        return sql, []
    if isinstance(args, dict):
        raise Untranslatable("named parameters (%(name)s)")
    args = list(args)
    out: list[str] = []
    flat: list = []
    i, n, L = 0, 0, len(sql)
    while i < L:
        ch = sql[i]
        if ch != "%":
            out.append(ch)
            i += 1
            continue
        nxt = sql[i + 1:i + 2]
        if nxt == "%":
            out.append("%")
        elif nxt == "s":
            if n >= len(args):
                raise TypeError("not enough arguments for format string")
            v = args[n]
            n += 1
            if isinstance(v, (list, tuple, set, frozenset)):
                parts = []
                for item in v:
                    parts.append(f"{MARK}{len(flat)}{MARK}")
                    flat.append(item)
                out.append("(" + ",".join(parts) + ")")
            else:
                out.append(f"{MARK}{len(flat)}{MARK}")
                flat.append(v)
        else:
            raise ValueError(f"unsupported format character {nxt!r}")
        i += 2
    if n != len(args):
        raise TypeError("not all arguments converted during string formatting")
    return "".join(out), flat


# --------------------------------------------------------------------------
# Step 2 — comments out, literals masked.

_ESC = {"0": "\0", "b": "\b", "n": "\n", "r": "\r", "t": "\t", "Z": "\x1a"}


def mask(sql: str) -> tuple[str, list[str]]:
    out: list[str] = []
    lits: list[str] = []
    i, L = 0, len(sql)
    while i < L:
        ch = sql[i]
        if ch in "'\"":
            # MariaDB reads both quotes as strings (no ANSI_QUOTES) and
            # honours backslash escapes; sqlite does neither, so the value is
            # decoded here and re-quoted the sqlite way.
            j, buf = i + 1, []
            while True:
                if j >= L:
                    raise Untranslatable("unterminated string literal")
                c = sql[j]
                if c == "\\" and j + 1 < L:
                    e = sql[j + 1]
                    # \% and \_ keep their backslash: it is LIKE's escape.
                    buf.append(_ESC.get(e, "\\" + e if e in "%_" else e))
                    j += 2
                    continue
                if c == ch:
                    if j + 1 < L and sql[j + 1] == ch:
                        buf.append(ch)
                        j += 2
                        continue
                    break
                if c == MARK:
                    raise Untranslatable("a parameter inside a string literal")
                buf.append(c)
                j += 1
            lits.append("'" + "".join(buf).replace("'", "''") + "'")
            out.append(f"{LIT}{len(lits) - 1}{LIT}")
            i = j + 1
        elif ch == "`":
            j = sql.find("`", i + 1)
            if j < 0:
                raise Untranslatable("unterminated `identifier`")
            out.append(sql[i:j + 1])          # sqlite reads backticks too
            i = j + 1
        elif sql.startswith("--", i) and (i + 2 >= L or sql[i + 2] in " \t\r\n"):
            j = sql.find("\n", i)
            i = L if j < 0 else j
        elif ch == "#":
            j = sql.find("\n", i)
            i = L if j < 0 else j
        elif sql.startswith("/*", i):
            if sql.startswith("/*!", i):
                raise Untranslatable("an executable /*! comment */")
            j = sql.find("*/", i + 2)
            if j < 0:
                raise Untranslatable("unterminated comment")
            out.append(" ")
            i = j + 2
        else:
            out.append(ch)
            i += 1
    return "".join(out), lits


def finish(masked: str, lits: list[str]) -> tuple[str, list[int]]:
    """Back to text: markers -> `?` (returning which argument each one is, in
    order), literals restored."""
    order: list[int] = []

    def mark(m):
        order.append(int(m.group(1)))
        return "?"
    s = re.sub(MARK + r"(\d+)" + MARK, mark, masked)
    s = re.sub(LIT + r"(\d+)" + LIT, lambda m: lits[int(m.group(1))], s)
    return s, order


# --------------------------------------------------------------------------
# Small parsing helpers. They run on masked text, so the only nesting left to
# respect is parentheses and backticks.

def _scan(s: str):
    """Yield (index, char, depth) outside backticks."""
    depth, i, L = 0, 0, len(s)
    while i < L:
        ch = s[i]
        if ch == "`":
            j = s.find("`", i + 1)
            i = L if j < 0 else j + 1
            continue
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        yield i, ch, depth
        i += 1


def split_top(s: str, sep: str = ",") -> list[str]:
    parts, last = [], 0
    for i, ch, depth in _scan(s):
        if ch == sep and depth == 0:
            parts.append(s[last:i])
            last = i + 1
    parts.append(s[last:])
    return [p.strip() for p in parts if p.strip()]


def find_kw(s: str, kw: str) -> int:
    """Index of keyword `kw` (spaces = any whitespace) at paren depth 0."""
    pat = re.compile(r"(?<![\w$`.])" + r"\s+".join(kw.split()) + r"(?![\w$])",
                     re.I)
    for i, ch, depth in _scan(s):
        if depth == 0 and pat.match(s, i):
            return i
    return -1


def matching_paren(s: str, start: int) -> int:
    for i, ch, depth in _scan(s):
        if i > start and ch == ")" and depth == 0:
            return i
    raise Untranslatable("unbalanced parentheses")


def bare(ident: str) -> str:
    return ident[1:-1] if ident.startswith("`") else ident


def q(ident: str) -> str:
    return "`" + bare(ident) + "`"


# --------------------------------------------------------------------------
# Expression rewrites shared by every DML statement.

def rewrite_expr(s: str) -> str:
    if "||" in s:
        # OR in MariaDB, concatenation in sqlite: same text, other meaning.
        raise Untranslatable("`||` (OR in MariaDB, concatenation in sqlite)")
    if re.search(r"(?<![\w$])JSON_\w+\s*\(", s, re.I):
        # json_extract and friends exist in both, and return differently
        # quoted values. Not guessable, so not guessed.
        raise Untranslatable("JSON_* functions (sqlite's differ in quoting)")
    if re.search(r"(?<![\w$])(REGEXP|RLIKE|SOUNDS\s+LIKE)(?![\w$])", s, re.I):
        raise Untranslatable("REGEXP")
    s = s.replace("<=>", " IS ")
    s = re.sub(r"(?<![\w$.])IF\s*\(", "IIF(", s, flags=re.I)
    # LENGTH is BYTES in MariaDB and characters in sqlite.
    s = re.sub(r"(?<![\w$.])LENGTH\s*\(", "octet_length(", s, flags=re.I)
    s = re.sub(r"(?<![\w$.])(?:CHAR|CHARACTER)_LENGTH\s*\(", "length(", s,
               flags=re.I)
    s = re.sub(r"(?<![\w$.])LAST_INSERT_ID\s*\(\s*\)", "last_insert_rowid()",
               s, flags=re.I)
    s = re.sub(r"(?<![\w$.])ROW_COUNT\s*\(\s*\)", "changes()", s, flags=re.I)
    s = re.sub(r"(?<![\w$.])CURRENT_USER(?:\s*\(\s*\))?(?![\w$])",
               "fm_current_user()", s, flags=re.I)
    for f in FUNCS:
        if f == "CURRENT_USER":
            continue
        s = re.sub(r"(?<![\w$.])" + f + r"\s*\(", f"fm_{f.lower()}(", s,
                   flags=re.I)
    # MariaDB's `/` is always exact division; sqlite's truncates two
    # integers. `* 1.0` before each `/` makes the left operand real without
    # moving anything: * and / share a precedence and associate left.
    s = re.sub(r"(?<![*/])/(?![*/])", "* 1.0 /", s)
    # sqlite has no default LIKE escape; MariaDB's is the backslash.
    s = re.sub(r"(?<![\w$])LIKE\s+((?:" + MARK + r"\d+" + MARK + "|" + LIT
               + r"\d+" + LIT + r"))(?!\s+ESCAPE)", r"LIKE \1 ESCAPE '\\'",
               s, flags=re.I)
    return s


# --------------------------------------------------------------------------
# Statements.

class Plan:
    """What the connection runs. `steps` are (kind, payload) pairs; the
    result of the LAST "sql" step is the statement's result."""

    def __init__(self):
        self.steps: list[tuple] = []

    def sql(self, masked: str, lits: list[str]) -> "Plan":
        text, order = finish(masked, lits)
        self.steps.append(("sql", (text, order)))
        return self

    def op(self, kind: str, *payload) -> "Plan":
        self.steps.append((kind, payload))
        return self


def translate(sql: str, args, found_rows: bool = False) -> tuple[Plan, list]:
    body, flat = bind_markers(sql, args)
    s, lits = mask(body)
    s = s.strip()
    while s.endswith(";"):
        s = s[:-1].rstrip()
    if ";" in s:
        raise Untranslatable("more than one statement in one call")
    if not s:
        raise Untranslatable("an empty statement")
    head = " ".join(s[:60].split()).upper()

    m = re.match(r"(CREATE|DROP)\s+(?:DATABASE|SCHEMA)\s+(IF\s+(?:NOT\s+)?EXISTS"
                 r"\s+)?(" + IDENT + r")(.*)$", s, re.I | re.S)
    if m:
        verb, cond, name = m.group(1).upper(), bool(m.group(2)), bare(m.group(3))
        return Plan().op("create_db" if verb == "CREATE" else "drop_db",
                         name, cond), flat
    if head.startswith("CREATE TABLE"):
        return create_table(s, lits), flat
    if head.startswith(("CREATE INDEX", "CREATE UNIQUE INDEX")):
        return create_index(s, lits), flat
    if head.startswith("ALTER TABLE"):
        return alter_table(s, lits), flat
    if head.startswith("DROP TABLE"):
        m = re.match(r"DROP\s+TABLE\s+(IF\s+EXISTS\s+)?(" + IDENT + r")\s*$",
                     s, re.I)
        if not m:
            raise Untranslatable("DROP TABLE of several tables")
        return (Plan().sql(s, lits)
                .op("forget_table", bare(m.group(2)))), flat
    if head.startswith("SET"):
        return set_statement(s), flat
    if head.startswith("OPTIMIZE TABLE"):
        names = [bare(x) for x in split_top(s[len("OPTIMIZE TABLE"):].strip())]
        return Plan().op("optimize", names), flat
    if head in ("BEGIN", "START TRANSACTION", "BEGIN WORK"):
        return Plan().op("begin"), flat
    if head in ("COMMIT", "COMMIT WORK"):
        return Plan().op("commit"), flat
    if head in ("ROLLBACK", "ROLLBACK WORK"):
        return Plan().op("rollback"), flat
    if head.startswith("TRUNCATE"):
        m = re.match(r"TRUNCATE\s+(?:TABLE\s+)?(" + IDENT + r")\s*$", s, re.I)
        if not m:
            raise Untranslatable("TRUNCATE")
        t = q(m.group(1))
        return (Plan().sql(f"DELETE FROM {t}", lits)
                .op("reset_sequence", bare(m.group(1)))), flat
    if head.split()[0] in ("SELECT", "INSERT", "UPDATE", "DELETE", "REPLACE",
                           "WITH", "VALUES"):
        return dml(s, lits, found_rows), flat
    raise Untranslatable(f"statement type {head.split()[0]!r}")


def set_statement(s: str) -> Plan:
    m = re.match(r"SET\s+(?:SESSION\s+|LOCAL\s+|@@SESSION\.|@@LOCAL\.|@@)?"
                 r"FOREIGN_KEY_CHECKS\s*(?:=|:=)\s*(\w+)\s*$", s, re.I)
    if m:
        v = m.group(1).upper()
        if v not in ("0", "1", "ON", "OFF"):
            raise Untranslatable(f"foreign_key_checks = {v}")
        return Plan().op("fk_checks", v in ("1", "ON"))
    if re.match(r"SET\s+NAMES\s", s, re.I):
        return Plan().op("noop")
    raise Untranslatable("SET of anything but foreign_key_checks / NAMES")


def dml(s: str, lits: list[str], found_rows: bool) -> Plan:
    plan = Plan()
    if re.search(r"(?<![\w$])information_schema\s*\.", s, re.I):
        plan.op("info_refresh")
    s = re.sub(r"^INSERT\s+IGNORE(?![\w$])", "INSERT OR IGNORE", s, flags=re.I)
    s = re.sub(r"\s+(?:FOR\s+UPDATE(?:\s+NOWAIT|\s+SKIP\s+LOCKED)?|LOCK\s+IN\s+"
               r"SHARE\s+MODE)\s*$", "", s, flags=re.I)
    s = rewrite_expr(s)

    up = find_kw(s, "ON DUPLICATE KEY UPDATE")
    if up >= 0:
        head = s[:up]
        tail = s[up:]
        tail = tail[re.match(r"ON\s+DUPLICATE\s+KEY\s+UPDATE", tail,
                             re.I).end():]
        pairs = [_assignment(a) for a in split_top(tail)]
        pairs = [(c, re.sub(r"(?<![\w$.])VALUES\s*\(\s*(" + IDENT + r")\s*\)",
                            lambda m: "excluded." + q(m.group(1)), e,
                            flags=re.I)) for c, e in pairs]
        sets = ", ".join(f"{c} = {e}" for c, e in pairs)
        s = f"{head.rstrip()} ON CONFLICT DO UPDATE SET {sets}"
        if not found_rows:
            # MariaDB reports an upsert that changes nothing as 0 rows.
            same = " AND ".join(f"{c} IS ({e})" for c, e in pairs)
            s += f" WHERE NOT ({same})"
    elif re.match(r"UPDATE\s", s, re.I):
        s = _update(s, found_rows)
    elif re.match(r"DELETE\s", s, re.I):
        if not re.match(r"DELETE\s+FROM\s", s, re.I):
            raise Untranslatable("multi-table DELETE")
        for kw in ("ORDER BY", "LIMIT"):
            if find_kw(s, kw) >= 0:
                raise Untranslatable(f"DELETE … {kw}")
    return plan.sql(s, lits)


def _assignment(a: str) -> tuple[str, str]:
    m = re.match(r"\s*(" + IDENT + r"(?:\." + IDENT + r")?)\s*=\s*(.+)$", a,
                 re.S)
    if not m:
        raise Untranslatable(f"assignment {a[:40]!r}")
    return m.group(1), m.group(2).strip()


def _update(s: str, found_rows: bool) -> str:
    """rowcount must be CHANGED rows (CLAUDE.md: CLIENT.FOUND_ROWS stays off),
    and sqlite counts matched ones. So a row whose every assignment would
    leave it as it is is excluded by the WHERE — the same end state, since
    MariaDB does not write such a row either — and changes() counts what
    MariaDB would."""
    i = find_kw(s, "SET")
    if i < 0:
        raise Untranslatable("UPDATE without SET")
    target = s[len("UPDATE"):i].strip()
    if find_kw(target, "JOIN") >= 0 or "," in target:
        raise Untranslatable("multi-table UPDATE")
    rest = s[i + 3:]
    for kw in ("ORDER BY", "LIMIT"):
        if find_kw(rest, kw) >= 0:
            raise Untranslatable(f"UPDATE … {kw}")
    w = find_kw(rest, "WHERE")
    sets_sql = rest if w < 0 else rest[:w]
    where = "" if w < 0 else rest[w + 5:].strip()
    pairs = [_assignment(a) for a in split_top(sets_sql)]
    # MariaDB evaluates SET left to right; sqlite reads every expression
    # against the OLD row. They differ only when an expression reads a column
    # assigned before it — refuse that instead of answering differently.
    for k, (col, expr) in enumerate(pairs):
        for prev, _ in pairs[:k]:
            name = re.escape(bare(prev.split(".")[-1]))
            if prev != col and re.search(r"(?<![\w$])`?" + name + r"`?(?![\w$])",
                                         expr, re.I):
                raise Untranslatable(
                    f"UPDATE whose {col} reads {prev}, assigned before it "
                    "(MariaDB: left to right; sqlite: the old row)")
    out = f"UPDATE {target} SET {sets_sql.strip()}"
    if found_rows:
        return out + (f" WHERE {where}" if where else "")
    same = " AND ".join(f"{c} IS ({e})" for c, e in pairs)
    if where:
        return f"{out} WHERE ({where}) AND NOT ({same})"
    return f"{out} WHERE NOT ({same})"


# --------------------------------------------------------------------------
# DDL.

def create_table(s: str, lits: list[str]) -> Plan:
    m = re.match(r"CREATE\s+TABLE\s+(IF\s+NOT\s+EXISTS\s+)?(" + IDENT + r")\s*\(",
                 s, re.I)
    if not m:
        raise Untranslatable("CREATE TABLE … LIKE / AS SELECT")
    table = bare(m.group(2))
    open_at = m.end() - 1
    close_at = matching_paren(s, open_at)
    items = split_top(s[open_at + 1:close_at])
    collation = _table_options(s[close_at + 1:])

    cols, keys, extra = [], [], []
    autoinc = None
    for item in items:
        up = " ".join(item.split()).upper()
        if up.startswith("PRIMARY KEY"):
            keys.append(("pk", item))
        elif re.match(r"(CONSTRAINT\s+\S+\s+)?UNIQUE\b", up):
            cm = re.search(r"\((.*)\)\s*$", item, re.S)
            if not cm:
                raise Untranslatable(f"UNIQUE {item[:40]!r}")
            keys.append(("uq", f"UNIQUE ({_index_cols(cm.group(1))})"))
        elif re.match(r"(KEY|INDEX)\b", up):
            km = re.match(r"(?:KEY|INDEX)\s+(" + IDENT + r")\s*\((.*)\)\s*$",
                          item, re.I | re.S)
            if not km:
                raise Untranslatable(f"index {item[:40]!r}")
            # sqlite index names are per database, MariaDB's per table.
            extra.append(f"CREATE INDEX IF NOT EXISTS "
                         f"`{table}__{bare(km.group(1))}` ON `{table}` "
                         f"({_index_cols(km.group(2))})")
        elif re.match(r"(CONSTRAINT\s+\S+\s+)?(FOREIGN\s+KEY|CHECK)\b", up):
            keys.append(("fk", item))
        elif re.match(r"(FULLTEXT|SPATIAL)\b", up):
            raise Untranslatable(up.split()[0] + " index")
        else:
            col, is_auto = _column(item, table)
            if is_auto:
                autoinc = bare(re.match(IDENT, item.strip()).group(0))
            cols.append(col)

    body = list(cols)
    for kind, text in keys:
        if kind == "pk" and autoinc:
            pm = re.search(r"\((.*)\)", text)
            if not pm or [bare(c.strip()) for c in pm.group(1).split(",")] \
                    != [autoinc]:
                raise Untranslatable("AUTO_INCREMENT that is not the whole "
                                     "primary key")
            continue
        body.append(text)
    has_pk = autoinc is not None or any(k == "pk" for k, _ in keys) or any(
        re.search(r"\bPRIMARY\s+KEY\b", c, re.I) for c in cols)
    # STRICT: MariaDB's strict mode refuses a value of the wrong type, and so
    # does a STRICT table. WITHOUT ROWID: stored in primary-key order, as
    # InnoDB clusters it, so an unordered scan comes back in the same order.
    opts = "STRICT" + (", WITHOUT ROWID" if has_pk and not autoinc else "")
    exists = "IF NOT EXISTS " if m.group(1) else ""
    plan = Plan()
    plan.op("meta")
    plan.sql(f"CREATE TABLE {exists}`{table}` (\n  " + ",\n  ".join(body)
             + f"\n) {opts}", lits)
    for x in extra:
        plan.sql(x, lits)
    plan.op("collation", table, collation, bool(m.group(1)))
    return plan


def _index_cols(cols: str) -> str:
    out = []
    for c in split_top(cols):
        cm = re.match(r"(" + IDENT + r")\s*(\(\s*\d+\s*\))?\s*(ASC|DESC)?\s*$",
                      c, re.I)
        if not cm:
            raise Untranslatable(f"index column {c!r}")
        if cm.group(2):
            raise Untranslatable("a prefix index (col(n))")
        out.append(q(cm.group(1)) + (" " + cm.group(3).upper()
                                     if cm.group(3) else ""))
    return ", ".join(out)


def _table_options(opts: str) -> str:
    collation = "utf8mb4_bin"           # dbharness creates every database so
    rest = opts                         # and MariaDB inherits it.
    for pat in (r"ENGINE\s*=?\s*\w+", r"(?:DEFAULT\s+)?(?:CHARSET|CHARACTER\s+"
                r"SET)\s*=?\s*\w+", r"ROW_FORMAT\s*=?\s*\w+",
                r"AUTO_INCREMENT\s*=?\s*\d+",
                r"COMMENT\s*=?\s*" + LIT + r"\d+" + LIT):
        rest = re.sub(pat, " ", rest, flags=re.I)
    cm = re.search(r"(?:DEFAULT\s+)?COLLATE\s*=?\s*(\w+)", rest, re.I)
    if cm:
        collation = cm.group(1)
        rest = rest[:cm.start()] + rest[cm.end():]
    if rest.strip(" ,\n\t"):
        raise Untranslatable(f"table option {rest.strip()[:40]!r}")
    return collation


def _column(item: str, table: str) -> tuple[str, bool]:
    m = re.match(r"(" + IDENT + r")\s+([A-Za-z]+)\s*(\(\s*[\d\s,]+\))?(.*)$",
                 item, re.S)
    if not m:
        raise Untranslatable(f"column {item[:40]!r}")
    name, typ, size, attrs = bare(m.group(1)), m.group(2).upper(), m.group(3), \
        m.group(4)
    n = int(size.strip("() ").split(",")[0]) if size else None
    checks = []
    if typ in ("VARCHAR", "CHAR"):
        sqlt = "TEXT"
        if n is not None:
            checks.append(f'CONSTRAINT "fm_len:{name}:{n}" CHECK '
                          f"(length(`{name}`) <= {n})")
    elif typ in TEXT_BYTES:
        sqlt = "TEXT"
        if TEXT_BYTES[typ]:
            checks.append(f'CONSTRAINT "fm_len:{name}:{TEXT_BYTES[typ]}" CHECK '
                          f"(octet_length(`{name}`) <= {TEXT_BYTES[typ]})")
    elif typ in INT_RANGE:
        sqlt = "INTEGER"
        rng = INT_RANGE[typ]
        if re.search(r"\bUNSIGNED\b", attrs, re.I):
            rng = None if rng is None else (0, rng[1] * 2 + 1)
        if rng:
            checks.append(f'CONSTRAINT "fm_range:{name}" CHECK (`{name}` '
                          f"BETWEEN {rng[0]} AND {rng[1]})")
    elif typ in REAL_TYPES:
        sqlt = "REAL"
    elif typ in BLOB_TYPES:
        sqlt = "BLOB"
    else:
        raise Untranslatable(f"column type {typ}")
    auto = bool(re.search(r"\bAUTO_INCREMENT\b", attrs, re.I))
    a = attrs
    for pat in (r"\bAUTO_INCREMENT\b", r"\bUNSIGNED\b", r"\bZEROFILL\b",
                r"\bCHARACTER\s+SET\s+\w+", r"\bCHARSET\s+\w+",
                r"\bCOLLATE\s+\w+", r"\bCOMMENT\s+" + LIT + r"\d+" + LIT):
        a = re.sub(pat, " ", a, flags=re.I)
    if re.search(r"\bON\s+UPDATE\b|\bGENERATED\b|\bAS\s*\(", a, re.I):
        raise Untranslatable("ON UPDATE / generated columns")
    a = " ".join(a.split())
    if auto:
        if sqlt != "INTEGER":
            raise Untranslatable("AUTO_INCREMENT on a non-integer")
        a = re.sub(r"\bNOT\s+NULL\b", "", a, flags=re.I).strip()
        return (f"`{name}` INTEGER PRIMARY KEY AUTOINCREMENT"
                + (" " + a if a else "")), True
    return (f"`{name}` {sqlt}" + (" " + a if a else "")
            + "".join(" " + c for c in checks)), False


def create_index(s: str, lits: list[str]) -> Plan:
    m = re.match(r"CREATE\s+(UNIQUE\s+)?INDEX\s+(IF\s+NOT\s+EXISTS\s+)?("
                 + IDENT + r")\s+ON\s+(" + IDENT + r")\s*\((.*)\)\s*$",
                 s, re.I | re.S)
    if not m:
        raise Untranslatable("CREATE INDEX form")
    t = bare(m.group(4))
    return Plan().sql(f"CREATE {'UNIQUE ' if m.group(1) else ''}INDEX "
                      f"{'IF NOT EXISTS ' if m.group(2) else ''}"
                      f"`{t}__{bare(m.group(3))}` ON `{t}` "
                      f"({_index_cols(m.group(5))})", lits)


def alter_table(s: str, lits: list[str]) -> Plan:
    m = re.match(r"ALTER\s+TABLE\s+(" + IDENT + r")\s+ADD\s+(?:COLUMN\s+)?"
                 r"(IF\s+NOT\s+EXISTS\s+)?(" + IDENT + r"\s+.+?)"
                 r"(?:\s+(?:AFTER\s+" + IDENT + r"|FIRST))?\s*$", s, re.I | re.S)
    if m:
        table = bare(m.group(1))
        coldef, auto = _column(m.group(3), table)
        if auto:
            raise Untranslatable("ADD COLUMN … AUTO_INCREMENT")
        name = bare(re.match(IDENT, m.group(3)).group(0))
        text, order = finish(f"ALTER TABLE `{table}` ADD COLUMN {coldef}", lits)
        # AFTER/FIRST only places the column; sqlite appends it. Nothing in
        # the broker reads a row by position.
        return Plan().op("add_column", table, name, bool(m.group(2)),
                         (text, order))
    m = re.match(r"ALTER\s+TABLE\s+(" + IDENT + r")\s+CONVERT\s+TO\s+(?:"
                 r"CHARACTER\s+SET|CHARSET)\s+\w+(?:\s+COLLATE\s+(\w+))?\s*$",
                 s, re.I)
    if m:
        # Recorded, and reported by information_schema — which is what the
        # doctor reads. Comparisons stay binary: a test that needs a
        # case-INSENSITIVE table to behave as one needs a real server.
        return Plan().op("collation", bare(m.group(1)),
                         m.group(2) or "utf8mb4_general_ci", False)
    raise Untranslatable("ALTER TABLE form")
