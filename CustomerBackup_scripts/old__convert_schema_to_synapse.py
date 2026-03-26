"""
convert_schema_to_synapse.py
-----------------------------
Converts an Azure SQL Database schema script into a Synapse Dedicated SQL Pool
compatible schema by:
  - Removing unsupported constraints (FOREIGN KEY, CHECK, UNIQUE constraints)
  - Removing unsupported index syntax
  - Replacing unsupported data types (XML → NVARCHAR(MAX), etc.)
  - Adding WITH (DISTRIBUTION = ..., CLUSTERED COLUMNSTORE INDEX) to each table
  - Auto-detecting a candidate HASH distribution column (first INT primary key)
  - Writing a clean output file + a report of all changes made

Usage:
    python convert_schema_to_synapse.py --input schema.sql --output synapse_schema.sql
"""

import re
import argparse
from pathlib import Path


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Data types not supported in Synapse → replaced with alternatives
UNSUPPORTED_TYPES = {
    r'\bXML\b':                     'NVARCHAR(MAX)',
    r'\bGEOGRAPHY\b':               'NVARCHAR(MAX)',
    r'\bGEOMETRY\b':                'NVARCHAR(MAX)',
    r'\bHIERARCHYID\b':             'NVARCHAR(MAX)',
    r'\bSQL_VARIANT\b':             'NVARCHAR(MAX)',
    r'\bTIMESTAMP\b':               'BINARY(8)',
    r'\bROWVERSION\b':              'BINARY(8)',
    r'\bIMAGE\b':                   'VARBINARY(MAX)',
    r'\bTEXT\b':                    'NVARCHAR(MAX)',
    r'\bNTEXT\b':                   'NVARCHAR(MAX)',
}

# Lines/blocks to remove entirely (case-insensitive patterns)
LINES_TO_REMOVE = [
    r'^\s*ALTER TABLE .+ ADD CONSTRAINT .+ FOREIGN KEY',
    r'^\s*ALTER TABLE .+ WITH (CHECK|NOCHECK) ADD CONSTRAINT',
    r'^\s*ALTER TABLE .+ CHECK CONSTRAINT',
    r'^\s*ALTER TABLE .+ NOCHECK CONSTRAINT',
    r'^\s*CREATE\s+(UNIQUE\s+)?(NONCLUSTERED|CLUSTERED)\s+INDEX',
    r'^\s*GO\s*$',                    # remove GO statements (not needed in Synapse)
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def detect_distribution_column(columns_block: str) -> str | None:
    """
    Try to find a good HASH distribution candidate:
    1. First INT / BIGINT column that looks like a primary/foreign key (ends in Id)
    2. Fallback: first INT / BIGINT column
    Returns the column name or None.
    """
    # Look for columns ending in 'id' of integer type
    id_pattern = re.findall(
        r'\[?(\w*[Ii][Dd]\w*)\]?\s+(?:INT|BIGINT|SMALLINT)',
        columns_block
    )
    if id_pattern:
        return id_pattern[0]

    # Fallback: any INT column
    any_int = re.findall(
        r'\[?(\w+)\]?\s+(?:INT|BIGINT)',
        columns_block
    )
    if any_int:
        return any_int[0]

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
    # Remove PRIMARY KEY constraints defined as separate lines inside CREATE TABLE
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
    source = Path(input_path).read_text(encoding='utf-8', errors='replace')

    report = []
    warnings = []

    # 1. Replace unsupported data types
    report.append("\n[1] Data type replacements:")
    source = replace_unsupported_types(source, report)

    # 2. Remove unsupported ALTER TABLE / INDEX lines
    report.append("\n[2] Removed unsupported statements:")
    cleaned_lines = []
    removed_count = 0
    for line in source.splitlines():
        removed = False
        for pattern in LINES_TO_REMOVE:
            if re.match(pattern, line, flags=re.IGNORECASE):
                removed = True
                removed_count += 1
                break
        if not removed:
            cleaned_lines.append(line)
    source = '\n'.join(cleaned_lines)
    report.append(f"  Removed {removed_count} unsupported lines (FOREIGN KEY, INDEX, GO, etc.)")

    # 3. Process each CREATE TABLE block
    report.append("\n[3] CREATE TABLE conversions:")
    table_pattern = re.compile(
        r'CREATE\s+TABLE\s+\[?\w+\]?(?:\.\[?\w+\]?)?\s*\(.*?\)\s*;?',
        re.IGNORECASE | re.DOTALL
    )
    source = table_pattern.sub(
        lambda m: convert_create_table(m, report, warnings),
        source
    )

    # 4. Write output
    Path(output_path).write_text(source, encoding='utf-8')

    # 5. Print report
    print("=" * 60)
    print("  SYNAPSE SCHEMA CONVERSION REPORT")
    print("=" * 60)
    for line in report:
        print(line)

    if warnings:
        print("\n[⚠  Warnings — review these tables manually:]")
        for w in warnings:
            print(w)

    print("\n" + "=" * 60)
    print(f"✅ Output written to: {output_path}")
    print("=" * 60)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Convert SQL Server schema to Azure Synapse.')
    parser.add_argument('--input',  required=True, help='Path to your original schema.sql file')
    parser.add_argument('--output', required=True, help='Path for the converted synapse_schema.sql file')
    args = parser.parse_args()

    convert_schema(args.input, args.output)
