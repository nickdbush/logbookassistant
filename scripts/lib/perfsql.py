"""Parse UTF-16BE encoded .perfsql files into lists of dicts.

Format:
    -- (comment)
    insert IGNORE into TABLE (col1, col2, ...) VALUES (?, ?, ...)
    (val1, 'val2', null)
    (val3, 'val4', null)

Values: single-quoted strings (with \', \\, \n, \r, \t escapes),
unquoted null, unquoted numbers (int or float).
"""

from __future__ import annotations

import re


def parse_perfsql(
    data: bytes,
    columns: list[str] | None = None,
) -> list[dict]:
    """Parse a perfsql file (raw bytes) into a list of dicts.

    Args:
        data: Raw bytes of the .perfsql file (UTF-16BE encoded).
        columns: Optional list of column names to extract. If provided,
                 only these columns are included in the output dicts.

    Returns:
        List of dicts keyed by column name.
    """
    text = data.decode("utf-16-be")
    if text and text[0] == "\ufeff":
        text = text[1:]

    lines = text.split("\n")

    # Find the INSERT line to extract column names
    col_names = None
    data_start = 0
    for i, line in enumerate(lines):
        stripped = line.strip()
        if stripped.lower().startswith("insert"):
            col_names = _parse_insert_columns(stripped)
            data_start = i + 1
            break

    if col_names is None:
        return []

    # Build column index filter
    if columns:
        col_set = set(c.lower() for c in columns)
        keep_indices = [
            i for i, name in enumerate(col_names) if name.lower() in col_set
        ]
    else:
        keep_indices = None

    rows = []
    for i in range(data_start, len(lines)):
        line = lines[i].strip()
        if not line or line.startswith("--"):
            continue
        if not line.startswith("("):
            continue
        values = _parse_data_row(line)
        if values is None:
            continue
        if len(values) != len(col_names):
            continue
        if keep_indices is not None:
            row = {col_names[j]: values[j] for j in keep_indices}
        else:
            row = dict(zip(col_names, values))
        rows.append(row)

    return rows


def _parse_insert_columns(insert_line: str) -> list[str]:
    """Extract column names from an INSERT line."""
    # Match content inside first parentheses
    m = re.search(r"\(\s*(.+?)\s*\)\s*(?:VALUES|values)", insert_line)
    if not m:
        return []
    cols_str = m.group(1)
    return [c.strip().upper() for c in cols_str.split(",")]


def _parse_data_row(line: str) -> list | None:
    """Parse a data row like (val1, 'val2', null) into a list of values."""
    if not line.startswith("(") or not line.endswith(")"):
        return None

    inner = line[1:-1]
    values = []
    i = 0
    n = len(inner)

    while i < n:
        # Skip whitespace
        while i < n and inner[i] in " \t":
            i += 1
        if i >= n:
            break

        if inner[i] == "'":
            # Quoted string
            val, i = _parse_quoted_string(inner, i)
            values.append(val)
        elif inner[i:i+4].lower() == "null":
            values.append(None)
            i += 4
        else:
            # Number or other unquoted value
            j = i
            while j < n and inner[j] not in ",)":
                j += 1
            token = inner[i:j].strip()
            if token.lower() == "null":
                values.append(None)
            else:
                values.append(_parse_number(token))
            i = j

        # Skip comma
        while i < n and inner[i] in " \t":
            i += 1
        if i < n and inner[i] == ",":
            i += 1

    return values


def _parse_quoted_string(s: str, start: int) -> tuple[str, int]:
    """Parse a single-quoted string starting at position start."""
    i = start + 1  # skip opening quote
    chars = []
    n = len(s)

    while i < n:
        c = s[i]
        if c == "\\" and i + 1 < n:
            nc = s[i + 1]
            if nc == "'":
                chars.append("'")
            elif nc == "\\":
                chars.append("\\")
            elif nc == "n":
                chars.append("\n")
            elif nc == "r":
                chars.append("\r")
            elif nc == "t":
                chars.append("\t")
            else:
                chars.append(nc)
            i += 2
        elif c == "'":
            i += 1
            break
        else:
            chars.append(c)
            i += 1

    return "".join(chars), i


def _parse_number(token: str):
    """Parse a numeric token, returning int or float."""
    try:
        return int(token)
    except ValueError:
        pass
    try:
        return float(token)
    except ValueError:
        return token
