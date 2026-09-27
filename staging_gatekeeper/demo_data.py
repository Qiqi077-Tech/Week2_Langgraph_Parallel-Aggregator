"""Demo schema and staging batches for the mock database.

The dirty batch is seeded with one deliberate defect per check; row ids referenced in
tests and in the README are stable.
"""

from __future__ import annotations

from datetime import date
from typing import Any

from .mock_db import MockStagingDatabase
from .models import ColumnSpec, ForeignKey, HierarchySpec, TableSchema

TENANT_ID = 42
OTHER_TENANT_ID = 7
STAGING_TABLE = "dbo.STG_DEPARTMENT"

BATCH_DIRTY = "BATCH-2026-0926-001"
BATCH_WARNING = "BATCH-2026-0926-002"
BATCH_CLEAN = "BATCH-2026-0926-003"

DEPARTMENT_SCHEMA = TableSchema(
    table=STAGING_TABLE,
    master_table="dbo.DEPARTMENT",
    business_key=["DEPARTMENTCODE"],
    effective_start="EFFECTIVESTARTDATE",
    effective_end="EFFECTIVEENDDATE",
    columns=[
        ColumnSpec(name="DEPARTMENTCODE", data_type="varchar", nullable=False, max_length=7,
                   format="department_code", format_severity="CRITICAL"),
        ColumnSpec(name="DEPARTMENTNAME", data_type="varchar", nullable=False, max_length=30),
        ColumnSpec(name="COSTCENTERID", data_type="int", nullable=False),
        ColumnSpec(name="PARENTDEPARTMENTCODE", data_type="varchar", max_length=7),
        ColumnSpec(name="MANAGEREMAIL", data_type="varchar", max_length=60, format="email"),
        ColumnSpec(name="HEADCOUNT", data_type="smallint", min_value=0, max_value=5000),
        ColumnSpec(name="EFFECTIVESTARTDATE", data_type="date", nullable=False),
        ColumnSpec(name="EFFECTIVEENDDATE", data_type="date"),
    ],
    foreign_keys=[ForeignKey(column="COSTCENTERID", parent_table="dbo.COSTCENTER", parent_column="COSTCENTERID")],
    hierarchy=HierarchySpec(key_column="DEPARTMENTCODE", parent_column="PARENTDEPARTMENTCODE"),
)

COSTCENTERS = [
    {"TENANT_ID": TENANT_ID, "COSTCENTERID": 100},
    {"TENANT_ID": TENANT_ID, "COSTCENTERID": 200},
    {"TENANT_ID": TENANT_ID, "COSTCENTERID": 300},
    {"TENANT_ID": OTHER_TENANT_ID, "COSTCENTERID": 999},  # exists, but not for tenant 42
]


def _master(code: str, parent: str | None, start: date, end: date | None = None) -> dict[str, Any]:
    return {"TENANT_ID": TENANT_ID, "DEPARTMENTCODE": code, "PARENTDEPARTMENTCODE": parent,
            "EFFECTIVESTARTDATE": start, "EFFECTIVEENDDATE": end}


DEPARTMENTS = [
    _master("ENG-001", None, date(2020, 1, 1)),
    _master("FIN-001", "ENG-001", date(2020, 1, 1)),
    _master("OPS-001", "ENG-001", date(2023, 1, 1)),
]

_next_id = 0


def _row(batch: str, code: Any, name: Any = "Dept", cc: Any = 100, parent: Any = None,
         email: Any = None, headcount: Any = 10, start: Any = date(2024, 1, 1), end: Any = None,
         tenant: int = TENANT_ID, row_id: int | None = None) -> dict[str, Any]:
    global _next_id
    _next_id = row_id if row_id is not None else _next_id + 1
    return {
        "STAGE_ROW_ID": _next_id, "BATCH_ID": batch, "TENANT_ID": tenant,
        "DEPARTMENTCODE": code, "DEPARTMENTNAME": name, "COSTCENTERID": cc,
        "PARENTDEPARTMENTCODE": parent, "MANAGEREMAIL": email, "HEADCOUNT": headcount,
        "EFFECTIVESTARTDATE": start, "EFFECTIVEENDDATE": end,
    }


def build_staging_rows() -> list[dict[str, Any]]:
    global _next_id
    _next_id = 0
    d, w, c = BATCH_DIRTY, BATCH_WARNING, BATCH_CLEAN
    rows = [
        # --- dirty batch (rows 1-21); ids are stable ---
        _row(d, "HRD-001", "Human Resources", 100, email="hr@corp.com"),                      # 1 clean
        _row(d, "LEG-001", "Legal", 200, parent="HRD-001", email="legal@corp.com"),           # 2 clean, parent in batch
        _row(d, "MKT-001", None, 100),                                                        # 3 NULL name
        _row(d, "mkt-002", "Marketing East", 100),                                            # 4 bad code format
        _row(d, "SAL-001", "Sales Operations and Regional Partnerships Division", 100,
             email="not-an-email"),                                                           # 5 name too long + bad email (warning)
        _row(d, "SAL-002", "Sales West", 100, headcount=99999),                               # 6 headcount out of range
        _row(d, "SAL-003", "Sales South", 100, headcount="twelve", start="2024-13-45"),       # 7 type mismatches
        _row(d, "PRJ-001", "Projects", 555, parent="ZZZ-999"),                                # 8 missing cost center + orphan parent
        _row(d, "PRJ-002", "Projects Two", 999),                                              # 9 cost center of another tenant
        _row(d, "CYA-001", "Cycle A", 100, parent="CYB-001"),                                 # 10 cycle A>B>C>A
        _row(d, "CYB-001", "Cycle B", 100, parent="CYC-001"),                                 # 11
        _row(d, "CYC-001", "Cycle C", 100, parent="CYA-001"),                                 # 12
        _row(d, "SLF-001", "Self Parent", 100, parent="SLF-001"),                             # 13 self loop
        _row(d, "DUP-001", "Dup First", 100),                                                 # 14 duplicate key
        _row(d, "DUP-001", "Dup Second", 100, end=date(2024, 6, 30)),                         # 15
        _row(d, "OVL-001", "Overlap A", 100, start=date(2024, 1, 1), end=date(2024, 12, 31)),  # 16 windows overlap
        _row(d, "OVL-001", "Overlap B", 100, start=date(2024, 7, 1)),                         # 17
        _row(d, "OPS-001", "Operations", 100, start=date(2023, 1, 1)),                        # 18 key exists in master
        _row(d, "ENG-001", "Engineering v2", 100, start=date(2025, 1, 1)),                    # 19 overlaps master version (warning)
        _row(d, "BAD-001", "Bad Window", 100, start=date(2024, 1, 1), end=date(2023, 1, 1)),  # 20 end before start
        _row(d, "TIE-001", "Tie", 100, parent="ENG-001"),                                     # 21 clean, parent in master
        # --- warning-only batch ---
        _row(w, "HRD-001", "Human Resources", 100, email="hr@corp.com", row_id=101),
        _row(w, "LEG-001", "Legal", 200, email="legal at corp", row_id=102),                  # bad email
        # --- clean batch ---
        _row(c, "HRD-001", "Human Resources", 100, email="hr@corp.com", row_id=201),
        _row(c, "LEG-001", "Legal", 200, parent="HRD-001", row_id=202),
        # --- noise that must never leak into tenant 42 / dirty batch results ---
        _row("BATCH-OTHER", "DUP-001", None, 555, row_id=301),
        _row(d, "DUP-001", None, 555, tenant=OTHER_TENANT_ID, row_id=302),
    ]
    return rows


def build_demo_database(*, latency: float = 0.05, transient_failures: int = 0) -> MockStagingDatabase:
    return MockStagingDatabase(
        schemas=[DEPARTMENT_SCHEMA],
        staging={STAGING_TABLE: build_staging_rows()},
        masters={"dbo.COSTCENTER": COSTCENTERS, "dbo.DEPARTMENT": DEPARTMENTS},
        latency=latency,
        transient_failures=transient_failures,
    )
