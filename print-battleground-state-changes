#!/usr/bin/env python3

import csv
import datetime
import email.utils
import git
import hashlib
import itertools
import os
import simdjson
import subprocess
import json
from textwrap import dedent, indent
from typing import Dict, List, NamedTuple
from tabulate import tabulate
from collections import defaultdict

# Constants
BATTLEGROUND_STATES = ["Alaska", "Arizona", "Georgia", "North Carolina", "Nevada", "Pennsylvania"]
CACHE_DIR = '_cache'
CACHE_VERSION = 2

class InputRecord(NamedTuple):
    timestamp: datetime.datetime
    state_name: str
    state_abbrev: str
    electoral_votes: int
    candidates: List[Dict[str, int]]
    votes: int
    expected_votes: int
    precincts_total: int
    precincts_reporting: int
    counties: Dict[str, int]

def git_commits_for(path: str) -> List[str]:
    """Get commit hashes for a specified file."""
    return subprocess.check_output(['git', 'log', '--format=%H', path]).strip().decode().splitlines()

def git_show(ref: str, name: str, repo_client) -> bytes:
    """Show the file content for a specific commit reference."""
    return repo_client.commit(ref).tree[name].data_stream.read()

def to_python_type(data):
    """Recursively convert simdjson objects to native Python types."""
    if isinstance(data, simdjson.Object):
        return {k: to_python_type(v) for k, v in data.items()}
    elif isinstance(data, simdjson.Array):
        return [to_python_type(item) for item in data]
    return data

def process_json_data(json_data: dict) -> List[InputRecord]:
    """Parse JSON data and create InputRecords."""
    records = []
    for race in json_data.get("races", []):
        updated_at = datetime.datetime.fromisoformat(race.get("updated_at").replace("Z", "+00:00"))
        for unit in race.get("reporting_units", []):
            candidates = [{"last_name": c.get("nyt_id", ""), "votes": c["votes"]["total"]} for c in unit.get("candidates", [])]
            record = InputRecord(
                timestamp=updated_at,
                state_name=unit.get("name", "Unknown"),
                state_abbrev=unit.get("state_abb", "Unknown"),
                electoral_votes=race.get("electoral_votes", 0),
                candidates=candidates,
                votes=unit.get("total_votes", 0),
                expected_votes=unit.get("total_expected_vote", 0),
                precincts_total=unit.get("precincts_total", 0),
                precincts_reporting=unit.get("precincts_reporting", 0),
                counties={}
            )
            records.append(record)
    return records

def fetch_all_records() -> Dict[str, List[InputRecord]]:
    """Fetch records from the commit history and cache them."""
    commits = git_commits_for("results.json")
    repo = git.Repo('.', odbt=git.db.GitCmdObjectDB)
    parser = simdjson.Parser()
    out = []

    for ref in commits:
        cache_path = os.path.join(CACHE_DIR, ref[:2], ref[2:] + ".json")
        if os.path.exists(cache_path):
            with open(cache_path) as fh:
                try:
                    record = json.load(fh)
                except ValueError:
                    continue
                if record.get('version') == CACHE_VERSION:
                    out.extend(InputRecord(*row) for row in record.get('rows', []))
                    continue

        blob = git_show(ref, 'results.json', repo)
        json_data = to_python_type(parser.parse(blob))

        rows = process_json_data(json_data)
        out.extend(rows)

        os.makedirs(os.path.dirname(cache_path), exist_ok=True)
        rows_for_cache = [
            {**row._asdict(), 'timestamp': row.timestamp.isoformat() if isinstance(row.timestamp, datetime.datetime) else row.timestamp}
            for row in rows
        ]
        with open(cache_path, 'w') as fh:
            json.dump({"version": CACHE_VERSION, "rows": rows_for_cache}, fh)

    out.sort(key=lambda row: row.timestamp)
    grouped = defaultdict(list)
    for row in out:
        grouped[row.state_name].append(row)

    return grouped

def compute_hurdle_sma(summarized_state_data, newest_votes, new_partition_pct, trailing_candidate_name):
    """Calculate the moving average hurdle rate."""
    MIN_AGG_VOTES = 30000
    agg_votes, agg_c2_votes = newest_votes, round(new_partition_pct * newest_votes)
    step = 0
    while step < len(summarized_state_data) and agg_votes < MIN_AGG_VOTES:
        this_summary = summarized_state_data[step]
        step += 1
        if this_summary.new_votes_relevant > 0:
            trailing_candidate_partition = (this_summary.trailing_candidate_partition
                                            if this_summary.trailing_candidate_name == trailing_candidate_name
                                            else this_summary.leading_candidate_partition)
            add_votes = min(this_summary.new_votes_relevant, MIN_AGG_VOTES - agg_votes)
            agg_votes += add_votes
            agg_c2_votes += round(trailing_candidate_partition * add_votes)
    return float(agg_c2_votes) / agg_votes if agg_votes else None

def generate_txt_output(path, summarized, states_updated):
    with open(path, "w") as f:
        print(tabulate([
            ["Last updated:", datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")],
            ["Latest batch received:", f"({', '.join(states_updated)})"],
            ["Web version:", "https://example.com"]
        ]), file=f)
        for state, timestamped_results in sorted(summarized.items()):
            print(f'\n{state} - Total Votes:', file=f)
            print(tabulate([string_summary(summary) for summary in timestamped_results]), file=f)

def generate_csv_output(path, summarized):
    with open(path, 'w') as csvfile:
        wr = csv.writer(csvfile)
        wr.writerow(('state',) + InputRecord._fields)
        for state, results in summarized.items():
            for row in results:
                wr.writerow((state,) + row)

def generate_rss_output(path, summarized):
    with open(path, 'w') as rssfile:
        print(dedent(f'''
            <?xml version="1.0" encoding="UTF-8"?>
            <rss version="2.0">
            <channel>
              <title>Election Results Feed</title>
              <link>https://example.com</link>
              <description>Latest results</description>
              <lastBuildDate>{email.utils.formatdate(datetime.datetime.utcnow().timestamp())}</lastBuildDate>
        '''), file=rssfile)

        for state, results in summarized.items():
            if not results:
                continue
            timestamp = results[0].timestamp.timestamp()
            print(indent(dedent(f'''
                <item>
                    <description>{state}: {results[0].candidates[0]["last_name"]} +{results[0].votes}</description>
                    <pubDate>{email.utils.formatdate(timestamp)}</pubDate>
                    <guid isPermaLink="false">{state}@{timestamp}</guid>
                </item>
            '''), "  "), file=rssfile)

        print(dedent('''
             </channel>
            </rss>'''), file=rssfile)

# Main script
if __name__ == "__main__":
    records = fetch_all_records()
    summarized = defaultdict(list)

    # Generate outputs
    generate_txt_output("battleground-state-changes.txt", summarized, BATTLEGROUND_STATES)
    generate_csv_output("battleground-state-changes.csv", summarized)
    generate_rss_output("battleground-state-changes.xml", summarized)

    print("Script completed successfully.")