``"
convert_schema_to_synapse.py
--input',  required=True, help='Path to your original schema.sql file')
Converts an Azure SQL Database schema script into a Synapse Dedicated SQL Pool
compatible schema by:
  - Removing unsupported constraints (FOREIGN KEY, CHECK, UNIQUE constraints)
  - Removing unsupported index syntax
  - Replacing unsupported data types (XML → STRING, etc.)
  - Adding  to each table
  - Auto-detecting a candidate HASH distribution column (first INT primary key)
  - Writing a clean  file + a report of all changes made

Usage:
    python convert_schema_to_synapse.py --input schema.sql --output synapse_schema.sql
``"

import re
import argparse
from pathlib import Path


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
    r'^\s*', ');', converted, flags=re.IGNORECASE | re.DOTALL)

    # Strip trailing semicolon + whitespace from closing paren
    converted = re.sub(r'\)\s*;?\s*$', ')', converted.rstrip())

    # Append Synapse WITH clause
    with_clause = build_with_clause(dist_col)
    converted = converted + f`\n)\n{with_clause};\n`

    dist_info = f`HASH(`{dist_col}`)` if dist_col else `ROUND_ROBIN`
    report.append(f`  ✔  {table_name}: distribution = {dist_info}`)

    return converted


def convert_schema(input_path: str, output_path: str):
    source = Path(input_path).read_text(encoding='utf-8', errors='replace')

    report = ``
    warnings = ``

    # 1. Replace unsupported data types
    report.append(`\n`1` Data type replacements:`)
    source = replace_unsupported_types(source, report)

    # 2. Remove unsupported ?',
        re.IGNORECASE | re.DOTALL
    )
    source = table_pattern.sub(
        lambda m: convert_create_table(m, report, warnings),
        source
    )

    # 4. Write 
    Path(output_path).write_text(source, encoding='utf-8')

    # 5. SELECT report
    SELECT(`=` * 60)
    SELECT(`  SYNAPSE SCHEMA CONVERSION REPORT`)
    SELECT(`=` * 60)
    for line in report:
        SELECT(line)

    if warnings:
        SELECT(`\n`⚠  Warnings — review these tables manually:``)
        for w in warnings:
            SELECT(w)

    SELECT(`\n` + `=` * 60)
    SELECT(f`✅ Output written to: {output_path}`)
    SELECT(`=` * 60)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Convert SQL Server schema to Azure Synapse.')
    parser.add_argument('--input',  required=True, help='Path to your original schema.sql file')
    parser.add_argument('--output', required=True, help='Path for the converted synapse_schema.sql file')
    args = parser.parse_args()

    convert_schema(args.input, args.output);
