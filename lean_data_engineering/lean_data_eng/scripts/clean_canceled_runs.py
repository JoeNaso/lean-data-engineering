"""
Python interface into Dagster GraphQL endspoints

Currently suports querying for specific job by name and deleting the respective runs 
"""
import datetime
import functools
import time

import click

from gql import Client, gql
from gql.transport.requests import RequestsHTTPTransport


CANCELED_RUNS = """
query FilteredRunsQuery {
  runsOrError(filter: { statuses: [CANCELED], pipelineName: "core_rollup_job"}) {
    __typename
    ... on Runs {
      results {
        runId
        jobName
        status
        startTime
      }
    }
  }
}
"""

MUTATION = """
mutation CloudMutation ($RUN_ID: String!) {
  deletePipelineRun(runId: $RUN_ID) {
    __typename
    ... on DeletePipelineRunSuccess {
        runId
    }
    ... on RunNotFoundError {
        runId
        message
    }
  }
}
"""

def calc_time(func):
    """
    Decorator to calc length of run time
    """
    @functools.wraps(func)
    def time_wrapper(*args, **kwargs):
        start = time.perf_counter()
        print(f"Start Time:\t{datetime.datetime.now().replace(microsecond=0).isoformat()}")
        result = func(*args, **kwargs)
        end = time.perf_counter()
        minutes, seconds = divmod(end - start, 60)
        print(f"\n{func.__qualname__}: {minutes} mins, {round(seconds)} seconds")
        return result

    return time_wrapper


@click.command()
@click.option("--url", type=str, help="URL of Dagster instance (http://localhost:3000)")
@click.option(
    "--api-key", type=str, help="API Key to authenticate with non-localhost deployments"
)
@click.option("--clobber", is_flag=True, default=False)
@calc_time
def cancel(url, api_key, clobber):
    instance_url = f"{url}/graphql"
    click.echo(f"Using URL:\t{instance_url}")
    headers = (
        {"Dagster-Cloud-Api-Token": api_key}
        if "localhost" not in url and api_key
        else {}
    )
    click.echo("Dispatching CANCELED_RUNS query...\n")
    transport = RequestsHTTPTransport(url=instance_url, headers=headers)
    client = Client(transport=transport, fetch_schema_from_transport=True)
    results = client.execute(gql(CANCELED_RUNS))
    run_ids = [r.get("runId") for r in results.get("runsOrError", {}).get("results")]
    run_cnt = len(run_ids)
    click.echo(f"Found {run_cnt} runs to delete...\n")
    if not clobber:
        click.echo("--clobber flag not provided.\nExiting...")
        return

    if run_cnt < 1:
        click.echo("No runs to delete.\nExiting...")
        return

    cnt = 0
    for rid in run_ids:
        params = {"RUN_ID": rid}
        _ = client.execute(gql(MUTATION), variable_values=params)
        if run_cnt <= 100:    
            click.echo(f"Deleted RunId: {rid}")
        
        cnt += 1
        if cnt % 100 == 0:
            click.echo(f"Running Total, Runs Deleted:\t{cnt}\n")
    
    click.echo(f"Deleted {cnt} runs. Exiting...")


if __name__ == "__main__":
    cancel()
