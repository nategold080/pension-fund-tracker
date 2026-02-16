"""Database module for pension fund tracker.

Manages SQLite database creation, migrations, and CRUD operations.
Schema designed to migrate to PostgreSQL trivially.
"""

import sqlite3
import uuid
from datetime import datetime
from pathlib import Path
from typing import Optional


DEFAULT_DB_PATH = Path("data/pension_tracker.db")

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS funds (
    id TEXT PRIMARY KEY,
    fund_name TEXT NOT NULL,
    fund_name_raw TEXT NOT NULL,
    general_partner TEXT,
    general_partner_normalized TEXT,
    vintage_year INTEGER,
    asset_class TEXT,
    sub_strategy TEXT,
    fund_size_mm REAL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS pension_funds (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    full_name TEXT,
    state TEXT,
    total_aum_mm REAL,
    website_url TEXT,
    data_source_type TEXT,
    disclosure_quality TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS commitments (
    id TEXT PRIMARY KEY,
    pension_fund_id TEXT REFERENCES pension_funds(id),
    fund_id TEXT REFERENCES funds(id),
    commitment_mm REAL,
    vintage_year INTEGER,
    capital_called_mm REAL,
    capital_distributed_mm REAL,
    remaining_value_mm REAL,
    net_irr REAL,
    net_multiple REAL,
    dpi REAL,
    as_of_date DATE,
    status TEXT,
    source_url TEXT NOT NULL,
    source_document TEXT,
    source_page INTEGER,
    extraction_method TEXT NOT NULL,
    extraction_confidence REAL,
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(pension_fund_id, fund_id, as_of_date)
);

CREATE TABLE IF NOT EXISTS fund_aliases (
    id TEXT PRIMARY KEY,
    fund_id TEXT REFERENCES funds(id),
    alias TEXT NOT NULL,
    source_pension_fund_id TEXT REFERENCES pension_funds(id),
    UNIQUE(alias, source_pension_fund_id)
);

CREATE TABLE IF NOT EXISTS gp_aliases (
    id TEXT PRIMARY KEY,
    canonical_name TEXT NOT NULL,
    alias TEXT NOT NULL,
    UNIQUE(alias)
);

CREATE TABLE IF NOT EXISTS extraction_runs (
    id TEXT PRIMARY KEY,
    pension_fund_id TEXT REFERENCES pension_funds(id),
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    status TEXT,
    records_extracted INTEGER,
    records_updated INTEGER,
    records_flagged INTEGER,
    errors TEXT,
    source_url TEXT,
    source_hash TEXT
);

CREATE TABLE IF NOT EXISTS review_queue (
    id TEXT PRIMARY KEY,
    commitment_id TEXT REFERENCES commitments(id),
    flag_type TEXT,
    flag_detail TEXT,
    resolved BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""


def generate_id() -> str:
    """Generate a UUID for use as a primary key."""
    return str(uuid.uuid4())


class Database:
    """SQLite database manager for the pension fund tracker."""

    def __init__(self, db_path: Optional[str | Path] = None):
        self.db_path = Path(db_path) if db_path else DEFAULT_DB_PATH
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn: Optional[sqlite3.Connection] = None

    @property
    def conn(self) -> sqlite3.Connection:
        if self._conn is None:
            self._conn = sqlite3.connect(str(self.db_path))
            self._conn.row_factory = sqlite3.Row
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA foreign_keys=ON")
        return self._conn

    def close(self):
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def migrate(self):
        """Create all tables if they don't exist. Safe to run repeatedly."""
        self.conn.executescript(SCHEMA_SQL)
        self.conn.commit()

    # ---- Pension Funds ----

    def upsert_pension_fund(
        self,
        id: str,
        name: str,
        full_name: Optional[str] = None,
        state: Optional[str] = None,
        total_aum_mm: Optional[float] = None,
        website_url: Optional[str] = None,
        data_source_type: Optional[str] = None,
        disclosure_quality: Optional[str] = None,
    ) -> str:
        """Insert or update a pension fund record."""
        now = datetime.utcnow().isoformat()
        self.conn.execute(
            """INSERT INTO pension_funds (id, name, full_name, state, total_aum_mm,
                website_url, data_source_type, disclosure_quality, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                name=excluded.name,
                full_name=excluded.full_name,
                state=excluded.state,
                total_aum_mm=excluded.total_aum_mm,
                website_url=excluded.website_url,
                data_source_type=excluded.data_source_type,
                disclosure_quality=excluded.disclosure_quality,
                updated_at=excluded.updated_at
            """,
            (id, name, full_name, state, total_aum_mm, website_url,
             data_source_type, disclosure_quality, now, now),
        )
        self.conn.commit()
        return id

    def get_pension_fund(self, id: str) -> Optional[dict]:
        """Get a pension fund by ID."""
        row = self.conn.execute(
            "SELECT * FROM pension_funds WHERE id = ?", (id,)
        ).fetchone()
        return dict(row) if row else None

    def list_pension_funds(self) -> list[dict]:
        """List all pension funds."""
        rows = self.conn.execute("SELECT * FROM pension_funds").fetchall()
        return [dict(r) for r in rows]

    # ---- Funds ----

    def upsert_fund(
        self,
        id: str,
        fund_name: str,
        fund_name_raw: str,
        general_partner: Optional[str] = None,
        general_partner_normalized: Optional[str] = None,
        vintage_year: Optional[int] = None,
        asset_class: Optional[str] = None,
        sub_strategy: Optional[str] = None,
        fund_size_mm: Optional[float] = None,
    ) -> str:
        """Insert or update a fund record."""
        now = datetime.utcnow().isoformat()
        self.conn.execute(
            """INSERT INTO funds (id, fund_name, fund_name_raw, general_partner,
                general_partner_normalized, vintage_year, asset_class, sub_strategy,
                fund_size_mm, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                fund_name=excluded.fund_name,
                general_partner=excluded.general_partner,
                general_partner_normalized=excluded.general_partner_normalized,
                vintage_year=excluded.vintage_year,
                asset_class=excluded.asset_class,
                sub_strategy=excluded.sub_strategy,
                fund_size_mm=excluded.fund_size_mm,
                updated_at=excluded.updated_at
            """,
            (id, fund_name, fund_name_raw, general_partner,
             general_partner_normalized, vintage_year, asset_class, sub_strategy,
             fund_size_mm, now, now),
        )
        self.conn.commit()
        return id

    def get_fund(self, id: str) -> Optional[dict]:
        """Get a fund by ID."""
        row = self.conn.execute(
            "SELECT * FROM funds WHERE id = ?", (id,)
        ).fetchone()
        return dict(row) if row else None

    def get_fund_by_name(self, fund_name: str) -> Optional[dict]:
        """Get a fund by exact canonical name."""
        row = self.conn.execute(
            "SELECT * FROM funds WHERE fund_name = ?", (fund_name,)
        ).fetchone()
        return dict(row) if row else None

    def list_funds(self) -> list[dict]:
        """List all funds."""
        rows = self.conn.execute("SELECT * FROM funds").fetchall()
        return [dict(r) for r in rows]

    # ---- Commitments ----

    def upsert_commitment(
        self,
        pension_fund_id: str,
        fund_id: str,
        source_url: str,
        extraction_method: str,
        commitment_mm: Optional[float] = None,
        vintage_year: Optional[int] = None,
        capital_called_mm: Optional[float] = None,
        capital_distributed_mm: Optional[float] = None,
        remaining_value_mm: Optional[float] = None,
        net_irr: Optional[float] = None,
        net_multiple: Optional[float] = None,
        dpi: Optional[float] = None,
        as_of_date: Optional[str] = None,
        status: Optional[str] = None,
        source_document: Optional[str] = None,
        source_page: Optional[int] = None,
        extraction_confidence: Optional[float] = None,
    ) -> str:
        """Insert or update a commitment record. Uses UPSERT on unique constraint."""
        now = datetime.utcnow().isoformat()
        id = generate_id()
        self.conn.execute(
            """INSERT INTO commitments (id, pension_fund_id, fund_id, commitment_mm,
                vintage_year, capital_called_mm, capital_distributed_mm, remaining_value_mm,
                net_irr, net_multiple, dpi, as_of_date, status, source_url, source_document,
                source_page, extraction_method, extraction_confidence, extracted_at,
                created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(pension_fund_id, fund_id, as_of_date) DO UPDATE SET
                commitment_mm=excluded.commitment_mm,
                vintage_year=excluded.vintage_year,
                capital_called_mm=excluded.capital_called_mm,
                capital_distributed_mm=excluded.capital_distributed_mm,
                remaining_value_mm=excluded.remaining_value_mm,
                net_irr=excluded.net_irr,
                net_multiple=excluded.net_multiple,
                dpi=excluded.dpi,
                status=excluded.status,
                source_url=excluded.source_url,
                source_document=excluded.source_document,
                source_page=excluded.source_page,
                extraction_method=excluded.extraction_method,
                extraction_confidence=excluded.extraction_confidence,
                extracted_at=excluded.extracted_at,
                updated_at=excluded.updated_at
            """,
            (id, pension_fund_id, fund_id, commitment_mm, vintage_year,
             capital_called_mm, capital_distributed_mm, remaining_value_mm,
             net_irr, net_multiple, dpi, as_of_date, status, source_url,
             source_document, source_page, extraction_method, extraction_confidence,
             now, now, now),
        )
        self.conn.commit()
        # Return the actual ID (may be existing if upserted)
        row = self.conn.execute(
            """SELECT id FROM commitments
            WHERE pension_fund_id = ? AND fund_id = ? AND as_of_date IS ?""",
            (pension_fund_id, fund_id, as_of_date),
        ).fetchone()
        return row["id"] if row else id

    def get_commitments(
        self,
        pension_fund_id: Optional[str] = None,
        fund_id: Optional[str] = None,
    ) -> list[dict]:
        """Get commitments with optional filters."""
        query = "SELECT * FROM commitments WHERE 1=1"
        params = []
        if pension_fund_id:
            query += " AND pension_fund_id = ?"
            params.append(pension_fund_id)
        if fund_id:
            query += " AND fund_id = ?"
            params.append(fund_id)
        rows = self.conn.execute(query, params).fetchall()
        return [dict(r) for r in rows]

    def get_commitments_joined(self) -> list[dict]:
        """Get all commitments with fund and pension fund names joined."""
        rows = self.conn.execute(
            """SELECT c.*, f.fund_name, f.general_partner, f.asset_class,
                f.sub_strategy, p.name as pension_fund_name, p.state as pension_fund_state
            FROM commitments c
            JOIN funds f ON c.fund_id = f.id
            JOIN pension_funds p ON c.pension_fund_id = p.id
            ORDER BY p.name, f.fund_name"""
        ).fetchall()
        return [dict(r) for r in rows]

    def count_commitments(self, pension_fund_id: Optional[str] = None) -> int:
        """Count commitment records."""
        if pension_fund_id:
            row = self.conn.execute(
                "SELECT COUNT(*) as cnt FROM commitments WHERE pension_fund_id = ?",
                (pension_fund_id,),
            ).fetchone()
        else:
            row = self.conn.execute(
                "SELECT COUNT(*) as cnt FROM commitments"
            ).fetchone()
        return row["cnt"]

    # ---- Fund Aliases ----

    def add_fund_alias(
        self, fund_id: str, alias: str, source_pension_fund_id: Optional[str] = None
    ) -> str:
        """Add an alias for a fund. Returns the alias ID."""
        id = generate_id()
        try:
            self.conn.execute(
                """INSERT INTO fund_aliases (id, fund_id, alias, source_pension_fund_id)
                VALUES (?, ?, ?, ?)""",
                (id, fund_id, alias, source_pension_fund_id),
            )
            self.conn.commit()
        except sqlite3.IntegrityError:
            # Alias already exists for this source
            row = self.conn.execute(
                "SELECT id FROM fund_aliases WHERE alias = ? AND source_pension_fund_id IS ?",
                (alias, source_pension_fund_id),
            ).fetchone()
            return row["id"] if row else id
        return id

    def get_fund_aliases(self, fund_id: Optional[str] = None) -> list[dict]:
        """Get fund aliases, optionally filtered by fund_id."""
        if fund_id:
            rows = self.conn.execute(
                "SELECT * FROM fund_aliases WHERE fund_id = ?", (fund_id,)
            ).fetchall()
        else:
            rows = self.conn.execute("SELECT * FROM fund_aliases").fetchall()
        return [dict(r) for r in rows]

    def find_fund_by_alias(self, alias: str) -> Optional[dict]:
        """Find a fund by one of its aliases."""
        row = self.conn.execute(
            """SELECT f.* FROM funds f
            JOIN fund_aliases fa ON f.id = fa.fund_id
            WHERE fa.alias = ?""",
            (alias,),
        ).fetchone()
        return dict(row) if row else None

    # ---- GP Aliases ----

    def add_gp_alias(self, canonical_name: str, alias: str) -> str:
        """Add a GP alias mapping."""
        id = generate_id()
        try:
            self.conn.execute(
                "INSERT INTO gp_aliases (id, canonical_name, alias) VALUES (?, ?, ?)",
                (id, canonical_name, alias),
            )
            self.conn.commit()
        except sqlite3.IntegrityError:
            row = self.conn.execute(
                "SELECT id FROM gp_aliases WHERE alias = ?", (alias,)
            ).fetchone()
            return row["id"] if row else id
        return id

    def get_canonical_gp(self, alias: str) -> Optional[str]:
        """Get canonical GP name from alias."""
        row = self.conn.execute(
            "SELECT canonical_name FROM gp_aliases WHERE alias = ?", (alias,)
        ).fetchone()
        return row["canonical_name"] if row else None

    # ---- Extraction Runs ----

    def create_extraction_run(
        self,
        pension_fund_id: str,
        source_url: Optional[str] = None,
        source_hash: Optional[str] = None,
    ) -> str:
        """Create a new extraction run record."""
        id = generate_id()
        now = datetime.utcnow().isoformat()
        self.conn.execute(
            """INSERT INTO extraction_runs (id, pension_fund_id, started_at, status,
                source_url, source_hash)
            VALUES (?, ?, ?, 'running', ?, ?)""",
            (id, pension_fund_id, now, source_url, source_hash),
        )
        self.conn.commit()
        return id

    def complete_extraction_run(
        self,
        run_id: str,
        status: str,
        records_extracted: int = 0,
        records_updated: int = 0,
        records_flagged: int = 0,
        errors: Optional[str] = None,
    ):
        """Mark an extraction run as complete."""
        now = datetime.utcnow().isoformat()
        self.conn.execute(
            """UPDATE extraction_runs SET
                completed_at = ?, status = ?, records_extracted = ?,
                records_updated = ?, records_flagged = ?, errors = ?
            WHERE id = ?""",
            (now, status, records_extracted, records_updated, records_flagged,
             errors, run_id),
        )
        self.conn.commit()

    def get_last_extraction_run(self, pension_fund_id: str) -> Optional[dict]:
        """Get the most recent extraction run for a pension fund."""
        row = self.conn.execute(
            """SELECT * FROM extraction_runs
            WHERE pension_fund_id = ?
            ORDER BY started_at DESC LIMIT 1""",
            (pension_fund_id,),
        ).fetchone()
        return dict(row) if row else None

    def get_extraction_runs(self, pension_fund_id: Optional[str] = None) -> list[dict]:
        """Get extraction runs, optionally filtered."""
        if pension_fund_id:
            rows = self.conn.execute(
                "SELECT * FROM extraction_runs WHERE pension_fund_id = ? ORDER BY started_at DESC",
                (pension_fund_id,),
            ).fetchall()
        else:
            rows = self.conn.execute(
                "SELECT * FROM extraction_runs ORDER BY started_at DESC"
            ).fetchall()
        return [dict(r) for r in rows]

    # ---- Review Queue ----

    def add_review_item(
        self,
        commitment_id: str,
        flag_type: str,
        flag_detail: str,
    ) -> str:
        """Add an item to the review queue."""
        id = generate_id()
        self.conn.execute(
            """INSERT INTO review_queue (id, commitment_id, flag_type, flag_detail)
            VALUES (?, ?, ?, ?)""",
            (id, commitment_id, flag_type, flag_detail),
        )
        self.conn.commit()
        return id

    def get_review_queue(self, resolved: Optional[bool] = None) -> list[dict]:
        """Get review queue items."""
        if resolved is not None:
            rows = self.conn.execute(
                "SELECT * FROM review_queue WHERE resolved = ? ORDER BY created_at",
                (resolved,),
            ).fetchall()
        else:
            rows = self.conn.execute(
                "SELECT * FROM review_queue ORDER BY created_at"
            ).fetchall()
        return [dict(r) for r in rows]

    def resolve_review_item(self, id: str):
        """Mark a review queue item as resolved."""
        self.conn.execute(
            "UPDATE review_queue SET resolved = TRUE WHERE id = ?", (id,)
        )
        self.conn.commit()

    def clear_review_items_by_type(self, flag_type: str) -> int:
        """Delete all unresolved review items of a given type. Returns count deleted."""
        cursor = self.conn.execute(
            "DELETE FROM review_queue WHERE flag_type = ? AND resolved = FALSE",
            (flag_type,),
        )
        self.conn.commit()
        return cursor.rowcount

    def bulk_resolve_review_items(self, flag_type: str) -> int:
        """Mark all unresolved items of a given type as resolved. Returns count."""
        cursor = self.conn.execute(
            "UPDATE review_queue SET resolved = TRUE WHERE flag_type = ? AND resolved = FALSE",
            (flag_type,),
        )
        self.conn.commit()
        return cursor.rowcount

    def get_fuzzy_match_details(self) -> list[dict]:
        """Get fuzzy match review items joined with fund alias and canonical name info."""
        rows = self.conn.execute(
            """SELECT rq.id as review_id, rq.flag_detail, rq.commitment_id,
                fa.alias, f.fund_name as canonical_name, f.vintage_year,
                f.general_partner, fa.source_pension_fund_id
            FROM review_queue rq
            JOIN commitments c ON rq.commitment_id = c.id
            JOIN funds f ON c.fund_id = f.id
            LEFT JOIN fund_aliases fa ON fa.fund_id = f.id
                AND fa.source_pension_fund_id = c.pension_fund_id
            WHERE rq.flag_type = 'fuzzy_match' AND rq.resolved = FALSE
            ORDER BY f.fund_name"""
        ).fetchall()
        return [dict(r) for r in rows]
