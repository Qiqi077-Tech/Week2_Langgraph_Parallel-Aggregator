"""Database abstraction plus an in-memory simulator.

`StagingDatabase` is the only thing the graph nodes know about. `MockStagingDatabase`
answers each `CheckQuery` by interpreting its `kind` against in-memory tables, so the graph
runs with no server. A real implementation (pyodbc / aioodbc) would execute `query.sql`
with `query.params` and return the resulting rows as dicts.
"""

from __future__ import annotations

import asyncio
import re
from collections import defaultdict
from datetime import date, datetime
from typing import Any, Callable, Mapping, Protocol, Sequence

from .models import (
    BATCH_COLUMN,
    ROW_ID_COLUMN,
    TENANT_COLUMN,
    TableSchema,
    normalize_table_name,
)
from .queries import FORMAT_RULES, CheckQuery

Row = dict[str, Any]


class TransientDatabaseError(RuntimeError):
    """Timeouts, deadlocks, dropped connections: safe to retry."""


class UnknownTableError(ValueError):
    pass


class StagingDatabase(Protocol):
    async def get_table_schema(self, table: str) -> TableSchema: ...

    async def count_batch_rows(self, table: str, tenant_id: int, batch_id: str) -> int: ...

    async def run_check(self, query: CheckQuery) -> list[Row]:
        """Return the rows violating the check (each has at least STAGE_ROW_ID)."""
        ...


# --- value helpers (emulate TRY_CAST on text-typed staging columns) ----------------------

def is_blank(value: Any) -> bool:
    return value is None or (isinstance(value, str) and not value.strip())


def as_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        try:
            return int(value.strip())
        except ValueError:
            return None
    return None


def as_date(value: Any) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return date.fromisoformat(value.strip())
        except ValueError:
            return None
    return None


def norm_key(value: Any) -> Any:
    """Comparable form of a key value (case-insensitive like the default SQL collation)."""
    as_number = as_int(value)
    if as_number is not None:
        return as_number
    return str(value).strip().upper()


class MockStagingDatabase:
    def __init__(
        self,
        *,
        schemas: Sequence[TableSchema],
        staging: Mapping[str, Sequence[Row]],
        masters: Mapping[str, Sequence[Row]],
        latency: float = 0.05,
        transient_failures: int = 0,
    ) -> None:
        self._schemas = {normalize_table_name(s.table): s for s in schemas}
        self._staging = {normalize_table_name(k): list(v) for k, v in staging.items()}
        self._masters = {normalize_table_name(k): list(v) for k, v in masters.items()}
        self._latency = latency
        self._transient_failures = transient_failures
        self.executed: list[CheckQuery] = []  # audit trail of every query issued
        self.in_flight = 0
        self.peak_in_flight = 0  # >1 proves the workers really ran concurrently

    # --- StagingDatabase ---------------------------------------------------------------

    async def get_table_schema(self, table: str) -> TableSchema:
        await asyncio.sleep(self._latency)
        try:
            return self._schemas[normalize_table_name(table)]
        except KeyError:
            raise UnknownTableError(f"No schema registered for {table!r}") from None

    async def count_batch_rows(self, table: str, tenant_id: int, batch_id: str) -> int:
        await asyncio.sleep(self._latency)
        return len(self._batch_rows(normalize_table_name(table), tenant_id, batch_id))

    async def run_check(self, query: CheckQuery) -> list[Row]:
        self.in_flight += 1
        self.peak_in_flight = max(self.peak_in_flight, self.in_flight)
        try:
            await asyncio.sleep(self._latency)
            if self._transient_failures > 0:
                self._transient_failures -= 1
                raise TransientDatabaseError("simulated connection reset")
            self.executed.append(query)
            evaluator: Callable[[CheckQuery, list[Row]], list[Row]] = getattr(self, f"_eval_{query.kind}")
            return evaluator(query, self._batch_rows(query.table, query.tenant_id, query.batch_id))
        finally:
            self.in_flight -= 1

    # --- helpers -----------------------------------------------------------------------

    def _batch_rows(self, table: str, tenant_id: int, batch_id: str) -> list[Row]:
        return [
            r for r in self._staging.get(table, [])
            if r[BATCH_COLUMN] == batch_id and r[TENANT_COLUMN] == tenant_id
        ]

    def _master_rows(self, table: str, tenant_id: int) -> list[Row]:
        return [r for r in self._masters.get(normalize_table_name(table), []) if r[TENANT_COLUMN] == tenant_id]

    @staticmethod
    def _bad(rows: list[Row], column: str, is_bad: Callable[[Any], bool]) -> list[Row]:
        return [
            {ROW_ID_COLUMN: r[ROW_ID_COLUMN], "BAD_VALUE": r.get(column)}
            for r in rows
            if not is_blank(r.get(column)) and is_bad(r.get(column))
        ]

    # --- evaluators, one per CheckQuery.kind -------------------------------------------

    def _eval_null_check(self, q: CheckQuery, rows: list[Row]) -> list[Row]:
        col = q.params["column"]
        return [{ROW_ID_COLUMN: r[ROW_ID_COLUMN], "BAD_VALUE": r.get(col)} for r in rows if is_blank(r.get(col))]

    def _eval_type_check(self, q: CheckQuery, rows: list[Row]) -> list[Row]:
        parse = as_date if q.params["data_type"] == "date" else as_int
        return self._bad(rows, q.params["column"], lambda v: parse(v) is None)

    def _eval_length_check(self, q: CheckQuery, rows: list[Row]) -> list[Row]:
        limit = q.params["max_length"]
        return self._bad(rows, q.params["column"], lambda v: len(str(v).strip()) > limit)

    def _eval_range_check(self, q: CheckQuery, rows: list[Row]) -> list[Row]:
        lo, hi = q.params["min_value"], q.params["max_value"]
        return self._bad(rows, q.params["column"], lambda v: (n := as_int(v)) is not None and not lo <= n <= hi)

    def _eval_format_check(self, q: CheckQuery, rows: list[Row]) -> list[Row]:
        regex = FORMAT_RULES[q.params["format"]].regex
        return self._bad(rows, q.params["column"], lambda v: not regex.fullmatch(str(v)))

    def _eval_fk_check(self, q: CheckQuery, rows: list[Row]) -> list[Row]:
        parents = {norm_key(r[q.params["parent_column"]]) for r in self._master_rows(q.params["parent_table"], q.tenant_id)}
        return self._bad(rows, q.params["column"], lambda v: norm_key(v) not in parents)

    def _eval_hierarchy_orphan(self, q: CheckQuery, rows: list[Row]) -> list[Row]:
        key_col, parent_col = q.params["key_column"], q.params["parent_column"]
        known = {norm_key(r[key_col]) for r in rows if not is_blank(r.get(key_col))}
        known |= {norm_key(r[key_col]) for r in self._master_rows(q.params["master_table"], q.tenant_id)}
        return self._bad(rows, parent_col, lambda v: norm_key(v) not in known)

    def _eval_hierarchy_cycle(self, q: CheckQuery, rows: list[Row]) -> list[Row]:
        key_col, parent_col = q.params["key_column"], q.params["parent_column"]
        parent_of: dict[Any, Any] = {}
        for r in self._master_rows(q.params["master_table"], q.tenant_id):
            parent_of[norm_key(r[key_col])] = None if is_blank(r.get(parent_col)) else norm_key(r[parent_col])
        for r in rows:  # batch edges override master edges
            if not is_blank(r.get(key_col)):
                parent_of[norm_key(r[key_col])] = None if is_blank(r.get(parent_col)) else norm_key(r[parent_col])

        def loop_from(start: Any) -> list[Any] | None:
            path, node = [start], parent_of.get(start)
            while node is not None and node != start and node not in path:
                path.append(node)
                node = parent_of.get(node)
            return path + [start] if node == start else None

        result = []
        for r in rows:
            if is_blank(r.get(key_col)):
                continue
            loop = loop_from(norm_key(r[key_col]))
            if loop:
                result.append({ROW_ID_COLUMN: r[ROW_ID_COLUMN], "CYCLE_PATH": " > ".join(map(str, loop))})
        return result

    def _key_of(self, q: CheckQuery, r: Row) -> tuple[Any, ...] | None:
        start = as_date(r.get(q.params["start_column"]))
        parts = [r.get(c) for c in q.params["business_key"]]
        if start is None or any(is_blank(p) for p in parts):
            return None
        return (*(norm_key(p) for p in parts), start)

    @staticmethod
    def _key_text(key: tuple[Any, ...]) -> str:
        return " | ".join(k.isoformat() if isinstance(k, date) else str(k) for k in key)

    def _eval_invalid_window(self, q: CheckQuery, rows: list[Row]) -> list[Row]:
        result = []
        for r in rows:
            start, end = as_date(r.get(q.params["start_column"])), as_date(r.get(q.params["end_column"]))
            if start and end and end < start:
                result.append({ROW_ID_COLUMN: r[ROW_ID_COLUMN], "START_DT": start, "END_DT": end})
        return result

    def _eval_duplicate_key(self, q: CheckQuery, rows: list[Row]) -> list[Row]:
        groups: dict[tuple[Any, ...], list[Row]] = defaultdict(list)
        for r in sorted(rows, key=lambda r: r[ROW_ID_COLUMN]):
            if (key := self._key_of(q, r)) is not None:
                groups[key].append(r)
        return [
            {ROW_ID_COLUMN: r[ROW_ID_COLUMN], "KEY_VALUE": self._key_text(key), "DUP_RANK": rank}
            for key, members in groups.items() if len(members) > 1
            for rank, r in enumerate(members, start=1)
        ]

    def _eval_master_key_collision(self, q: CheckQuery, rows: list[Row]) -> list[Row]:
        existing = set()
        for m in self._master_rows(q.params["master_table"], q.tenant_id):
            existing.add(self._key_of(q, m))
        return [
            {ROW_ID_COLUMN: r[ROW_ID_COLUMN], "KEY_VALUE": self._key_text(key)}
            for r in rows if (key := self._key_of(q, r)) is not None and key in existing
        ]

    def _windows(self, q: CheckQuery, rows: list[Row]) -> list[tuple[Row, tuple[Any, ...], date, date]]:
        """(row, entity key, start, end) for rows with a parseable, non-inverted window; NULL end = open."""
        out = []
        for r in rows:
            start = as_date(r.get(q.params["start_column"]))
            raw_end = r.get(q.params["end_column"])
            end = date.max if is_blank(raw_end) else as_date(raw_end)
            parts = [r.get(c) for c in q.params["business_key"]]
            if start is None or end is None or end < start or any(is_blank(p) for p in parts):
                continue
            out.append((r, tuple(norm_key(p) for p in parts), start, end))
        return out

    def _eval_date_overlap(self, q: CheckQuery, rows: list[Row]) -> list[Row]:
        wins = sorted(self._windows(q, rows), key=lambda w: w[0][ROW_ID_COLUMN])
        result = []
        for i, (a, ka, a_start, a_end) in enumerate(wins):
            for b, kb, b_start, b_end in wins[i + 1:]:
                if ka == kb and a_start != b_start and a_start <= b_end and b_start <= a_end:
                    result.append({
                        ROW_ID_COLUMN: a[ROW_ID_COLUMN], "OTHER_ROW_ID": b[ROW_ID_COLUMN],
                        "START_DT": a_start, "END_DT": a_end, "OTHER_START": b_start, "OTHER_END": b_end,
                    })
        return result

    def _eval_master_date_overlap(self, q: CheckQuery, rows: list[Row]) -> list[Row]:
        master_wins = self._windows(q, self._master_rows(q.params["master_table"], q.tenant_id))
        result = []
        for r, key, start, end in self._windows(q, rows):
            for _, m_key, m_start, m_end in master_wins:
                if key == m_key and start != m_start and start <= m_end and m_start <= end:
                    result.append({
                        ROW_ID_COLUMN: r[ROW_ID_COLUMN], "START_DT": start, "END_DT": end,
                        "MASTER_START": m_start, "MASTER_END": m_end,
                    })
        return result
