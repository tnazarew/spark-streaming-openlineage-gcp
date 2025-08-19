import argparse
import json
import os
import time
from os.path import join
from proto import Message
from google.oauth2.service_account import Credentials
from google.cloud.datacatalog_lineage_v1 import LineageClient, SearchLinksRequest
from google.protobuf.json_format import ParseDict
from google.protobuf import struct_pb2

credentials = "/Users/tomasznazarewicz/gcp-open-lineage-testing-125ff83662a1.json"
parent = "projects/gcp-open-lineage-testing/locations/us"

client = LineageClient(credentials=Credentials.from_service_account_file(credentials))


def dump_api_state():
    processes_state, runs_state, events_state, links_state = get_api_state()

    os.makedirs("api_state", exist_ok=True)

    with open(join("api_state", "processes.json"), 'w') as f:
        json.dump(processes_state, f, indent=2)
    with open(join("api_state", "runs.json"), 'w') as f:
        json.dump(runs_state, f, indent=2)
    with open(join("api_state", "lineage_events.json"), 'w') as f:
        json.dump(events_state, f, indent=2)
    with open(join("api_state", "links.json"), 'w') as f:
        json.dump(links_state, f, indent=2)


def get_api_state():
    processes = [Message.to_dict(p) for p in client.list_processes(parent=parent)]
    runs = [Message.to_dict(r) for p in processes for r in client.list_runs(parent=p['name'])]
    lineage_events = [Message.to_dict(e) for r in runs for e in client.list_lineage_events(parent=r['name'])]
    links = [Message.to_dict(res) for le in lineage_events for link in le['links'] for res in get_links(link)]

    values = list({link["name"]: link for link in links}.values())
    return processes, runs, lineage_events, values


def get_links(link):
    return client.search_links(
        request=SearchLinksRequest(source=link["source"], target=link["target"], parent=parent))


def clean_up():
    processes = [x for x in client.list_processes(parent=parent)]
    for p in processes:
        client.delete_process(name=p.name)


def count_processes():
    print("not empty" if any(p for p in client.list_processes(parent=parent)) else "empty")


def main():
    parser = argparse.ArgumentParser(description="")
    parser.add_argument('--clean', action='store_true', help="clear the dataproc")
    parser.add_argument("--is-empty", action='store_true', help="print the number of processes")
    args = parser.parse_args()

    if args.is_empty:
        count_processes()
    else:
        dump_api_state()

    if args.clean:
        print('cleaning up')
        clean_up()


if __name__ == "__main__":
    main()
