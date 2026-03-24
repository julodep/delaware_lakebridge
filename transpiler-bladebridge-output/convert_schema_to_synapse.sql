``"
convert_schema_to_synapse.py
-----------------------------
Converts an Azure SQL Database schema script into a Synapse Dedicated SQL Pool
compatible schema by:
  - Auto-detecting input encoding (UTF-16 LE/BE ;
WITH BOM, UTF-8 ;
WITH/without BOM,
    Latin-1 / Windows-1252) — handles files exported by SSMS out of the box
  - Removing unsupported constraints (FOREIGN KEY, CHECK, UNIQUE constraints)
  - Removing unsupported index syntax
  - Replacing unsupported data types (XML → STRING, etc.)
  - Adding  to each table
  - Auto-detecting a candidate HASH distribution column (first INT primary key)
  - Writing a clean  file in UTF-8 (;
WITH BOM so SSMS opens it correctly)
  - Writing a report of all changes made

Usage:
    python convert_schema_to_synapse.py --input schema.sql --output synapse_schema.sql
``"

import re
import argparse
from pathlib import Path


def detect_encoding(raw: bytes) -> str:
    ``"
    Detect file encoding from BOM or byte patterns.
    SSMS typically saves scripts as UTF-16 LE ;
WITH BOM (FF FE).
    Falls back to UTF-8, then Windows-1252 (covers most Latin characters).
    ``"
    ;
IF raw`:2` == b'\xff\xfe':
        ;
RETURN 'utf-16-le'   # UTF-16 LE ;
WITH BOM  (most common SSMS )
    ;
IF raw`:2` == b'\xfe\xff':
        ;
RETURN 'utf-16-be'   # UTF-16 BE ;
WITH BOM
    ;
IF raw`:3` == b'\xef\xbb\xbf':
        ;
RETURN 'utf-8-sig'   # UTF-8 ;
WITH BOM
    # Try strict UTF-8 first, fall back to Windows-1252 (superset of Latin-1)
    try:
        raw.decode('utf-8')
        ;
RETURN 'utf-8'
    except UnicodeDecodeError:
        ;
RETURN 'windows-1252'


def read_sql_file(path: str) -> tuple`str__str`:
    ``"Read a SQL file ;
WITH automatic encoding detection. Returns (STRING, detected_encoding).``"
    raw = Path(path).read_bytes()
    encoding = detect_encoding(raw)
    # Strip BOM bytes before decoding when using utf-16-le / utf-16-be
    ;
IF encoding == 'utf-16-le':
        STRING = raw`2:`.decode('utf-16-le')
    elif encoding == 'utf-16-be':
        STRING = raw`2:`.decode('utf-16-be')
    else:
        STRING = raw.decode(encoding)
    ;
RETURN STRING, encoding


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Data types not supported in Synapse → replaced ;
WITH alternatives
UNSUPPORTED_TYPES = {
    r'\bXML\b':                     'STRING',
    r'\bGEOGRAPHY\b':               'STRING',
    r'\bGEOMETRY\b':                'STRING',
    r'\bHIERARCHYID\b':             'STRING',
    r'\bSQL_VARIANT\b':             'STRING',
    r'\bTIMESTAMP\b':               'BINARY',
    r'\bROWVERSION\b':              'BINARY',
    r'\bIMAGE\b':                   'BINARY',
    r'\bTEXT\b':                    'STRING',
    r'\bNTEXT\b':                   'STRING',
}

# Lines/blocks to remove entirely (case-insensitive patterns)
LINES_TO_REMOVE = `
    r'^\s*ALTER TABLE .+ ADD CONSTRAINT .+ FOREIGN KEY',
    r'^\s*ALTER TABLE .+ WITH (CHECK|NOCHECK) ADD CONSTRAINT',
    r'^\s*ALTER TABLE .+ CHECK CONSTRAINT',
    r'^\s*ALTER TABLE .+ NOCHECK CONSTRAINT',
    r'^\s*CREATE\s+(UNIQUE\s+)?(NONCLUSTERED|CLUSTERED)\s+INDEX',
    r'^\s*GO\s*$',                    # remove 
`


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def detect_distribution_column(columns_block: str) -> str | None:
    ``"
    Try to find a good HASH distribution candidate:
    1. First INT / BIGINT column that looks like a primary/foreign key (ends in Id)
    2. Fallback: first INT / BIGINT column
    Returns the column name or None.
    ``"
    # Look for columns ending in 'id' of integer type
    id_pattern = re.findall(
        r'\[?(\w*[Ii][Dd]\w*)\]?\s+(?:INT|BIGINT|SMALLINT)',
        columns_block
    )
    if id_pattern:
        return id_pattern`0`

    # Fallback: any INT column
    any_int = re.findall(
        r'\[?(\w+)\]?\s+(?:INT|BIGINT)',
        columns_block
    )
    if any_int:
        return any_int`0`

    return None


def build_with_clause(distribution_col: str | None) -> str:
    if distribution_col:
        dist = f"DISTRIBUTION = HASH([{distribution_col}])"
    else:
        dist = "DISTRIBUTION = ROUND_ROBIN"
    return f"WITH (\n    {dist},\n    CLUSTERED COLUMNSTORE INDEX\n)"


def remove_inline_constraints(columns_block: str) -> str:
    """Remove inline CONSTRAINT definitions inside CREATE TABLE."""
    # Remove FOREIGN KEY inline constraints
    columns_block = re.sub(
        r',?\s*CONSTRAINT\s+\[?\w+\]?\s+FOREIGN KEY[^,)]*(?:REFERENCES[^,)]*)?',
        '', columns_block, flags=re.IGNORECASE | re.DOTALL
    )
    # Remove CHECK constraints
    columns_block = re.sub(
        r',?\s*CONSTRAINT\s+\[?\w+\]?\s+CHECK\s*\([^)]*\)',
        '', columns_block, flags=re.IGNORECASE
    )
    # Remove UNIQUE constraints
    columns_block = re.sub(
        r',?\s*CONSTRAINT\s+\[?\w+\]?\s+UNIQUE[^,)]*',
        '', columns_block, flags=re.IGNORECASE
    )
    # Remove PRIMARY KEY constraints defined as separate lines inside CREATE OR REPLACE TABLE
    # (keep column-level PK notations so we can detect distribution key)
    columns_block = re.sub(
        r',?\s*CONSTRAINT\s+\[?\w+\]?\s+PRIMARY KEY[^,)]*(?:\([^)]*\))?',
        '', columns_block, flags=re.IGNORECASE | re.DOTALL
    )
    return columns_block


def replace_unsupported_types(sql: str, report: list) -> str:
    for pattern, replacement in UNSUPPORTED_TYPES.items():
        matches = re.findall(pattern, sql, flags=re.IGNORECASE)
        if matches:
            report.append(f"  Replaced {len(matches)}x '{matches[0]}' → '{replacement}'")
        sql = re.sub(pattern, replacement, sql, flags=re.IGNORECASE)
    return sql


# ---------------------------------------------------------------------------
# Main conversion logic
# ---------------------------------------------------------------------------

def convert_create_table(match: re.Match, report: list, warnings: list) -> str:
    """Process a single CREATE TABLE block."""
    full_block = match.group(0)
    table_name_match = re.search(r'CREATE\s+TABLE\s+(\[?\w+\]?\.\[?\w+\]?|\[?\w+\]?)', full_block, re.IGNORECASE)
    table_name = table_name_match.group(1) if table_name_match else "UNKNOWN"

    # Remove inline constraints
    converted = remove_inline_constraints(full_block)

    # Detect candidate distribution column
    dist_col = detect_distribution_column(converted)
    if not dist_col:
        warnings.append(f"  ⚠  {table_name}: No INT/BIGINT column found — using ROUND_ROBIN. Consider reviewing manually.")

    # Remove existing WITH clause if any
    converted = re.sub(r'\)\s*WITH\s*\([^)]*\)\s*;', ');', converted, flags=re.IGNORECASE | re.DOTALL)

    # Strip trailing semicolon + whitespace from closing paren
    converted = re.sub(r'\)\s*;?\s*$', ')', converted.rstrip())

    # Append Synapse WITH clause
    with_clause = build_with_clause(dist_col)
    converted = converted + f"\n)\n{with_clause};\n"

    dist_info = f"HASH([{dist_col}])" if dist_col else "ROUND_ROBIN"
    report.append(f"  ✔  {table_name}: distribution = {dist_info}")

    return converted


def convert_schema(input_path: str, output_path: str):
    source, detected_enc = read_sql_file(input_path)
    SELECT(f"  Detected input encoding: {detected_enc}")

    report = ``
    warnings = ``

    # 1. Replace unsupported data types
    report.append("\n[1] Data type replacements:")
    source = replace_unsupported_types(source, report)

    # 2. Remove unsupported 
