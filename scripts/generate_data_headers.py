#!/usr/bin/env python3
"""
Generate C header files from address_standardizer SQL data files.

Converts us_lex.sql, us_gaz.sql, us_rules.sql into static C arrays
that can be compiled directly into the DuckDB extension binary.

Usage:
    python scripts/generate_data_headers.py
"""

import re
import os

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_DIR = os.path.dirname(SCRIPT_DIR)
DATA_DIR = os.path.join(REPO_DIR, "data")
OUT_DIR = os.path.join(REPO_DIR, "src", "generated")


def escape_c_string(s: str) -> str:
    """Escape a string for use in a C string literal."""
    return s.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def parse_lex_sql(path: str) -> list[tuple[int, str, str, int]]:
    """Parse INSERT INTO us_lex/us_gaz VALUES (id, seq, word, stdword, token);"""
    pattern = re.compile(
        r"INSERT INTO \w+ VALUES\s*\((\d+),\s*(\d+),\s*'((?:[^']|'')*)',\s*'((?:[^']|'')*)',\s*(\d+)\)"
    )
    entries = []
    with open(path) as f:
        for line in f:
            m = pattern.search(line)
            if m:
                seq = int(m.group(2))
                word = m.group(3).replace("''", "'")
                stdword = m.group(4).replace("''", "'")
                token = int(m.group(5))
                entries.append((seq, word, stdword, token))
    return entries


def parse_rules_sql(path: str) -> list[str]:
    """Parse INSERT INTO us_rules VALUES (id, rule);"""
    pattern = re.compile(
        r"INSERT INTO \w+ VALUES\s*\((\d+),\s*'((?:[^']|'')*)'[^)]*\)"
    )
    entries = []
    with open(path) as f:
        for line in f:
            m = pattern.search(line)
            if m:
                rule = m.group(2).replace("''", "'")
                entries.append(rule)
    return entries


def generate_lex_header(name: str, entries: list[tuple[int, str, str, int]]) -> str:
    """Generate a C header for lexicon/gazetteer data."""
    upper = name.upper()
    lines = [
        f"/* Auto-generated from data/{name}.sql — do not edit */",
        f"#ifndef {upper}_DATA_H",
        f"#define {upper}_DATA_H",
        "",
        '#include "us_data_types.h"',
        "",
        f"static const lex_entry_t {upper}_DATA[] = {{",
    ]
    for seq, word, stdword, token in entries:
        lines.append(f'    {{{seq}, "{escape_c_string(word)}", "{escape_c_string(stdword)}", {token}}},')
    lines.append("};")
    lines.append(f"static const int {upper}_COUNT = {len(entries)};")
    lines.append("")
    lines.append(f"#endif /* {upper}_DATA_H */")
    lines.append("")
    return "\n".join(lines)


def generate_rules_header(entries: list[str]) -> str:
    """Generate a C header for rules data."""
    lines = [
        "/* Auto-generated from data/us_rules.sql — do not edit */",
        "#ifndef US_RULES_DATA_H",
        "#define US_RULES_DATA_H",
        "",
        '#include "us_data_types.h"',
        "",
        "static const rule_entry_t US_RULES_DATA[] = {",
    ]
    for rule in entries:
        lines.append(f'    {{"{escape_c_string(rule)}"}},')
    lines.append("};")
    lines.append(f"static const int US_RULES_COUNT = {len(entries)};")
    lines.append("")
    lines.append("#endif /* US_RULES_DATA_H */")
    lines.append("")
    return "\n".join(lines)


def main():
    os.makedirs(OUT_DIR, exist_ok=True)

    lex_entries = parse_lex_sql(os.path.join(DATA_DIR, "us_lex.sql"))
    with open(os.path.join(OUT_DIR, "us_lex_data.h"), "w") as f:
        f.write(generate_lex_header("us_lex", lex_entries))
    print(f"us_lex_data.h: {len(lex_entries)} entries")

    gaz_entries = parse_lex_sql(os.path.join(DATA_DIR, "us_gaz.sql"))
    with open(os.path.join(OUT_DIR, "us_gaz_data.h"), "w") as f:
        f.write(generate_lex_header("us_gaz", gaz_entries))
    print(f"us_gaz_data.h: {len(gaz_entries)} entries")

    rules_entries = parse_rules_sql(os.path.join(DATA_DIR, "us_rules.sql"))
    with open(os.path.join(OUT_DIR, "us_rules_data.h"), "w") as f:
        f.write(generate_rules_header(rules_entries))
    print(f"us_rules_data.h: {len(rules_entries)} entries")


if __name__ == "__main__":
    main()
