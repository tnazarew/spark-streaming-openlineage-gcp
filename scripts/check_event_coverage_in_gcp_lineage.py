#!/usr/bin/env python3
"""
Compare OpenLineage events with Google Cloud Data Catalog lineage events
to find events that don't have corresponding timestamps.
"""

import json
import sys
from datetime import datetime
from typing import Set, List, Dict, Any

def load_json_file(filepath: str) -> Any:
    """Load and parse a JSON file."""
    try:
        with open(filepath, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"Error: File {filepath} not found")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON from {filepath}: {e}")
        sys.exit(1)

def parse_timestamp(timestamp_str: str) -> datetime:
    """Parse ISO timestamp string to datetime object."""
    # Handle both formats: with and without microseconds
    try:
        if '.' in timestamp_str and timestamp_str.endswith('Z'):
            # Format: 2025-10-01T19:11:37.584Z
            return datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
        elif timestamp_str.endswith('Z'):
            # Format: 2025-10-01T19:12:00Z
            return datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
        else:
            return datetime.fromisoformat(timestamp_str)
    except ValueError as e:
        print(f"Warning: Could not parse timestamp '{timestamp_str}': {e}")
        return None

def load_openlineage_events(filepath: str) -> List[Dict[str, Any]]:
    """Load OpenLineage events from the JSON Lines file."""
    events = []
    try:
        with open(filepath, 'r') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    event = json.loads(line)
                    events.append(event)
                except json.JSONDecodeError as e:
                    print(f"Warning: Could not parse line {line_num} in {filepath}: {e}")
                    continue
    except FileNotFoundError:
        print(f"Error: File {filepath} not found")
        sys.exit(1)
    return events

def extract_lineage_timestamps(lineage_events: List[Dict[str, Any]]) -> Set[datetime]:
    """Extract all start_time timestamps from lineage events."""
    timestamps = set()
    
    for event in lineage_events:
        if 'start_time' in event:
            timestamp = parse_timestamp(event['start_time'])
            if timestamp:
                timestamps.add(timestamp)
    
    return timestamps

def find_unmatched_events(openlineage_events: List[Dict[str, Any]], 
                         lineage_timestamps: Set[datetime],
                         tolerance_seconds: int = 1) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Find OpenLineage events that don't have corresponding timestamps in lineage events.
    Uses a tolerance window to account for slight timing differences.
    Returns a tuple of (unmatched_events, matched_events).
    """
    unmatched_events = []
    matched_events = []

    for event in openlineage_events:
        if 'eventTime' not in event:
            continue
            
        event_time = parse_timestamp(event['eventTime'])
        if not event_time:
            continue
            
        # Extract job namespace and name
        job_namespace = "UNKNOWN"
        job_name = "UNKNOWN"
        if 'job' in event:
            job_namespace = event['job'].get('namespace', 'UNKNOWN')
            job_name = event['job'].get('name', 'UNKNOWN')

        # Extract runId from the nested run object
        run_id = "UNKNOWN"
        if 'run' in event and 'runId' in event['run']:
            run_id = event['run']['runId']

        event_info = {
            'eventTime': event['eventTime'],
            'eventType': event.get('eventType', 'UNKNOWN'),
            'runId': run_id,
            'jobNamespace': job_namespace,
            'jobName': job_name
        }

        # Check if there's a matching timestamp within tolerance
        found_match = False
        for lineage_time in lineage_timestamps:
            time_diff = abs((event_time - lineage_time).total_seconds())
            if time_diff <= tolerance_seconds:
                found_match = True
                break

        if found_match:
            matched_events.append(event_info)
        else:
            unmatched_events.append(event_info)

    return unmatched_events, matched_events

def calculate_job_name_statistics(matched_events: List[Dict[str, Any]],
                                  unmatched_events: List[Dict[str, Any]]) -> Dict[str, Dict[str, int]]:
    """Calculate matched vs unmatched statistics per unique job name."""
    job_stats = {}

    # Count matched events per job name
    for event in matched_events:
        job_name = event['jobName']
        if job_name not in job_stats:
            job_stats[job_name] = {'matched': 0, 'unmatched': 0}
        job_stats[job_name]['matched'] += 1

    # Count unmatched events per job name
    for event in unmatched_events:
        job_name = event['jobName']
        if job_name not in job_stats:
            job_stats[job_name] = {'matched': 0, 'unmatched': 0}
        job_stats[job_name]['unmatched'] += 1

    return job_stats

def main():
    """Main function to compare events and report unmatched ones."""
    openlineage_file = "events/StreamToStreamEnrichmentTest.json"
    lineage_file = "api_state/lineage_events.json"
    
    print("Loading OpenLineage events...")
    openlineage_events = load_openlineage_events(openlineage_file)
    print(f"Loaded {len(openlineage_events)} OpenLineage events")
    
    print("Loading Data Catalog lineage events...")
    lineage_events = load_json_file(lineage_file)
    print(f"Loaded {len(lineage_events)} lineage events")
    
    print("Extracting timestamps from lineage events...")
    lineage_timestamps = extract_lineage_timestamps(lineage_events)
    print(f"Found {len(lineage_timestamps)} unique lineage timestamps")
    
    print("Finding unmatched OpenLineage events...")
    unmatched_events, matched_events = find_unmatched_events(openlineage_events, lineage_timestamps)

    # Calculate job name statistics
    job_stats = calculate_job_name_statistics(matched_events, unmatched_events)

    print("\n" + "="*60)
    print(f"RESULTS: Found {len(unmatched_events)} unmatched and {len(matched_events)} matched OpenLineage events")
    print("="*60)
    
    # Display job name statistics
    print(f"\nJOB NAME STATISTICS:")
    print("-" * 80)
    print(f"{'Job Name':<50} {'Matched':<10} {'Unmatched':<10} {'Total':<10} {'Match %':<10}")
    print("-" * 80)

    # Sort by job name for consistent output
    for job_name in sorted(job_stats.keys()):
        stats = job_stats[job_name]
        matched_count = stats['matched']
        unmatched_count = stats['unmatched']
        total_count = matched_count + unmatched_count
        match_percentage = (matched_count / total_count * 100) if total_count > 0 else 0

        print(f"{job_name:<50} {matched_count:<10} {unmatched_count:<10} {total_count:<10} {match_percentage:<10.1f}%")

    print("-" * 80)
    total_events = len(matched_events) + len(unmatched_events)
    overall_match_percentage = (len(matched_events) / total_events * 100) if total_events > 0 else 0
    print(f"{'OVERALL':<50} {len(matched_events):<10} {len(unmatched_events):<10} {total_events:<10} {overall_match_percentage:<10.1f}%")

    if unmatched_events:
        print("\nUnmatched events (eventTime | eventType | runId | jobNamespace | jobName):")
        print("-" * 120)

        # Group by event type for better readability
        by_type = {}
        for event in unmatched_events:
            event_type = event['eventType']
            if event_type not in by_type:
                by_type[event_type] = []
            by_type[event_type].append({
                'eventTime': event['eventTime'],
                'runId': event['runId'],
                'jobNamespace': event['jobNamespace'],
                'jobName': event['jobName']
            })

        for event_type, events in by_type.items():
            print(f"\n{event_type} events ({len(events)}):")
            # Sort by eventTime
            sorted_events = sorted(events, key=lambda x: x['eventTime'])
            for event in sorted_events:
                print(f"  {event['eventTime']} | {event_type} | {event['runId']} | {event['jobNamespace']} | {event['jobName']}")

        print(f"\nDETAILED LIST (chronological order):")
        print("-" * 120)
        # Sort all events chronologically
        sorted_all = sorted(unmatched_events, key=lambda x: x['eventTime'])
        for event in sorted_all:
            print(f"{event['eventTime']} | {event['eventType']} | {event['runId']} | {event['jobNamespace']} | {event['jobName']}")

        print(f"\nSUMMARY:")
        print(f"Total unmatched events: {len(unmatched_events)}")
        for event_type, events in by_type.items():
            print(f"  {event_type}: {len(events)}")
    else:
        print("\nAll OpenLineage events have corresponding lineage events!")
    
    if matched_events:
        print("\nMatched events (eventTime | eventType | runId | jobNamespace | jobName):")
        print("-" * 120)

        # Group by event type for better readability
        by_type_matched = {}
        for event in matched_events:
            event_type = event['eventType']
            if event_type not in by_type_matched:
                by_type_matched[event_type] = []
            by_type_matched[event_type].append({
                'eventTime': event['eventTime'],
                'runId': event['runId'],
                'jobNamespace': event['jobNamespace'],
                'jobName': event['jobName']
            })

        for event_type, events in by_type_matched.items():
            print(f"\n{event_type} events ({len(events)}):")
            # Sort by eventTime
            sorted_events = sorted(events, key=lambda x: x['eventTime'])
            for event in sorted_events:
                print(f"  {event['eventTime']} | {event_type} | {event['runId']} | {event['jobNamespace']} | {event['jobName']}")

        print(f"\nDETAILED LIST OF MATCHED EVENTS (chronological order):")
        print("-" * 120)
        # Sort all matched events chronologically
        sorted_matched_all = sorted(matched_events, key=lambda x: x['eventTime'])
        for event in sorted_matched_all:
            print(f"{event['eventTime']} | {event['eventType']} | {event['runId']} | {event['jobNamespace']} | {event['jobName']}")

        print(f"\nSUMMARY OF MATCHED EVENTS:")
        print(f"Total matched events: {len(matched_events)}")
        for event_type, events in by_type_matched.items():
            print(f"  {event_type}: {len(events)}")
    else:
        print("\nNo matched events found.")

    print(f"\nComparison completed.")

if __name__ == "__main__":
    main()
