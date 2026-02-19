#!/usr/bin/env python3
"""Data lineage tracking for MACRO-DATA pipeline (L5).

Records a structured event to a JSONL lineage log each time a pipeline step
produces or consumes data. This creates an audit trail that answers:
  - Which FRED series version was used for a given run?
  - Did cache or fresh fetch produce the data?
  - What input files were read and what was their row count?

Usage:
    from lineage import record

    record(
        step='fetch_macro_from_fred',
        market='us',
        out_dir='out/us',
        outputs={'macro_actuals.csv': 142},
        extra={'fred_series_fetched': 58, 'cache_hits': 44, 'failed_series': []},
    )

The record is appended to `out/{market}/lineage.jsonl` (one JSON object per line).
"""
import json
import logging
import os
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)


def record(
    step: str,
    market: str,
    out_dir: str | None = None,
    inputs: dict[str, int] | None = None,
    outputs: dict[str, int] | None = None,
    status: str = 'success',
    extra: dict[str, Any] | None = None,
) -> None:
    """Append one lineage record to out/{market}/lineage.jsonl.

    Args:
        step:    Pipeline step name (e.g. 'fetch_macro_from_fred').
        market:  'us' or 'kr'.
        out_dir: Output directory (defaults to out/{market}).
        inputs:  Dict of {filename: row_count} for files read.
        outputs: Dict of {filename: row_count} for files written.
        status:  'success', 'partial', or 'failed'.
        extra:   Any additional key-value metadata (serializable to JSON).
    """
    entry = {
        'ts': datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
        'step': step,
        'market': market,
        'status': status,
        'inputs': inputs or {},
        'outputs': outputs or {},
    }
    if extra:
        entry.update(extra)

    log_dir = out_dir or os.path.join('out', market)
    os.makedirs(log_dir, exist_ok=True)
    lineage_path = os.path.join(log_dir, 'lineage.jsonl')

    try:
        with open(lineage_path, 'a', encoding='utf-8') as f:
            f.write(json.dumps(entry, ensure_ascii=False) + '\n')
        logger.debug('Lineage recorded: step=%s market=%s status=%s', step, market, status)
    except Exception as e:
        # Lineage write should never break the pipeline
        logger.warning('Failed to write lineage record: %s', e)


def read_lineage(out_dir: str) -> list[dict]:
    """Read all lineage records from a directory's lineage.jsonl."""
    path = os.path.join(out_dir, 'lineage.jsonl')
    if not os.path.exists(path):
        return []
    records = []
    with open(path, encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
    return records
