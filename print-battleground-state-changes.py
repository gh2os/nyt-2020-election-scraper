#!/usr/bin/env python3

import csv
import datetime
import email.utils
import git
import hashlib
import os
import simdjson
import subprocess
import json
from textwrap import dedent, indent
from typing import Dict, List, NamedTuple
from tabulate import tabulate
from collections import defaultdict

# Constants
BATTLEGROUND_STATES = ["Michigan", "Arizona", "Georgia", "North Carolina", "Nevada", "Pennsylvania", "Virginia", "Wisconsin"]
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

def parse_isoformat(timestamp_str):
    """Parse ISO format date string, compatible with Python < 3.7."""
    if hasattr(datetime.datetime, "fromisoformat"):
        return datetime.datetime.fromisoformat(timestamp_str)
    else:
        # Remove the trailing 'Z' for UTC if present and use strptime
        if timestamp_str.endswith("Z"):
            timestamp_str = timestamp_str[:-1]
        return datetime.datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%S.%f")

def fetch_all_records():
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
                    for row in record.get('rows', []):
                        row_data = {**row}
                        if isinstance(row_data['timestamp'], str):
                            row_data['timestamp'] = parse_isoformat(row_data['timestamp'])
                        out.append(InputRecord(**row_data))
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

    out.sort(key=lambda row: row.timestamp if isinstance(row.timestamp, datetime.datetime) else parse_isoformat(row.timestamp))
    grouped = defaultdict(list)
    for row in out:
        grouped[row.state_name].append(row)

    return grouped

def string_summary(record):
    """Generate a summary string for a given InputRecord."""
    timestamp_str = record.timestamp.strftime("%Y-%m-%d %H:%M")
    sorted_candidates = sorted(record.candidates, key=lambda x: x['votes'], reverse=True)
    leading_candidate = sorted_candidates[0] if sorted_candidates else None
    trailing_candidate = sorted_candidates[1] if len(sorted_candidates) > 1 else None

    leading_candidate_name = leading_candidate['last_name'] if leading_candidate else "N/A"
    trailing_candidate_name = trailing_candidate['last_name'] if trailing_candidate else "N/A"
    vote_differential = leading_candidate['votes'] - trailing_candidate['votes'] if leading_candidate and trailing_candidate else 0

    votes_remaining = record.expected_votes - record.votes if record.expected_votes > 0 else "Unknown"
    new_votes_formatted = f"{record.votes:,}"
    precincts_reporting = f"{record.precincts_reporting / record.precincts_total:.2%}" if record.precincts_total > 0 else "N/A"
    hurdle = "Unknown"
    hurdle_trend = "n/a"

    return [
        f"{timestamp_str}",
        f"{leading_candidate_name} leading by {vote_differential:,}",
        f"Votes remaining (est.): {votes_remaining}",
        f"Change: {new_votes_formatted}",
        f"Precincts reporting: {precincts_reporting}",
        f"Hurdle for trailing candidate: {hurdle}",
        f"Trend: {hurdle_trend}"
    ]

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

def html_table(summarized: dict) -> List[str]:
    """Generate HTML table rows for each state in summarized data."""
    html_output = []
    for state, timestamped_results in sorted(summarized.items()):
        # 'Alaska (3)' -> 'alaska', 'North Carolina (15)' -> 'north-carolina'
        state_slug = state.split('(')[0].strip().replace(' ', '-').lower()
        
        html_output.append(f"<div class='table-responsive'><table id='{state_slug}' class='table table-bordered'>")
        html_output.append(f"""
            <thead class="thead-light">
                <tr>
                    <th colspan="9" style="text-align:left;">
                        <span>{state}</span> - Electoral Votes: {timestamped_results[0].electoral_votes}
                    </th>
                </tr>
                <tr>
                    <th>Timestamp</th>
                    <th>Leading Candidate</th>
                    <th>Vote Margin</th>
                    <th>Votes Remaining (est.)</th>
                    <th>Change</th>
                    <th>Batch Breakdown</th>
                    <th>Batch Trend</th>
                    <th>Hurdle</th>
                </tr>
            </thead>
        """)

        for record in timestamped_results:
            # Sort candidates by votes to determine leading and trailing candidates
            sorted_candidates = sorted(record.candidates, key=lambda x: x['votes'], reverse=True)
            leading_candidate = sorted_candidates[0] if sorted_candidates else None
            trailing_candidate = sorted_candidates[1] if len(sorted_candidates) > 1 else None

            leading_candidate_name = leading_candidate['last_name'] if leading_candidate else "N/A"
            trailing_candidate_name = trailing_candidate['last_name'] if trailing_candidate else "N/A"
            vote_differential = leading_candidate['votes'] - trailing_candidate['votes'] if leading_candidate and trailing_candidate else 0

            # Estimated votes remaining
            votes_remaining = record.expected_votes - record.votes if record.expected_votes > 0 else "Unknown"

            # Format new votes in this update
            new_votes_formatted = f"{record.votes:,}"

            # Calculate precincts reporting as a percentage
            precincts_reporting = f"{record.precincts_reporting / record.precincts_total:.2%}" if record.precincts_total > 0 else "N/A"

            # Placeholder for hurdle and trend (if available)
            hurdle = "Unknown"
            hurdle_trend = "n/a"

            html_output.append(f"""
                <tr>
                    <td>{record.timestamp.strftime('%Y-%m-%d %H:%M:%S')}</td>
                    <td>{leading_candidate_name}</td>
                    <td>{vote_differential:,}</td>
                    <td>{votes_remaining}</td>
                    <td>{new_votes_formatted}</td>
                    <td>{precincts_reporting}</td>
                    <td>{hurdle_trend}</td>
                    <td>{hurdle}</td>
                </tr>
            """)
        html_output.append("</table></div><hr>")
    return html_output

def html_output(path: str, table_rows: List[str], states_updated: List[str], other_page_html: str):
    html_template = """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Election Results</title>
        <link rel="stylesheet" href="https://stackpath.bootstrapcdn.com/bootstrap/4.5.2/css/bootstrap.min.css">
    </head>
    <body>
        <div class="container">
            <h1>Election Results Summary</h1>
            <p>Last updated: {last_updated}</p>
            <p>{other_page_link}</p>
            <div>{table_content}</div>
        </div>
    </body>
    </html>
    """

    last_updated = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    other_page_link = f"Data for all 50 states and DC is <a href='all-state-changes.html'>also available</a>." if "battleground-state-changes.html" in path else "View <a href='battleground-state-changes.html'>battleground states only</a>."
    html_content = html_template.format(
        last_updated=last_updated,
        other_page_link=other_page_html,
        table_content="\n".join(table_rows)
    )

    with open(path, "w", encoding="utf-8") as f:
        f.write(html_content)

if __name__ == "__main__":
    records = fetch_all_records()
    summarized = records
    battlegrounds_summarized = {state: records[state] for state in BATTLEGROUND_STATES if state in records}
    battleground_states_updated = list(battlegrounds_summarized.keys())
    states_updated = list(summarized.keys())

    generate_txt_output("battleground-state-changes.txt", summarized, BATTLEGROUND_STATES)
    generate_csv_output("battleground-state-changes.csv", summarized)
    generate_rss_output("battleground-state-changes.xml", summarized)
    
    html_output(
        path="battleground-state-changes.html",
        table_rows=html_table(battlegrounds_summarized),
        states_updated=battleground_states_updated,
        other_page_html='Data for all 50 states and DC is <a href="all-state-changes.html">also available</a>.'
    )

    html_output(
        path="all-state-changes.html",
        table_rows=html_table(summarized),
        states_updated=states_updated,
        other_page_html='View <a href="battleground-state-changes.html">battleground states only</a>.'
    )

    print("Script completed successfully.")