from loguru import logger

from core import Report, list_to_df
from db import dataframe_to_db
from util import TASK_FIELDS


def get_relevant_issue_data(jira_report: Report):
    rows = []
    issues = jira_report.issues
    for issue in issues:
        issue_data = jira_report.get_issue_data(issue, TASK_FIELDS)
        sprint_data = jira_report.get_issue_field_val(issue, "customfield_10020")
        issue_data["last_sprint"] = jira_report.get_last_sprint(sprint_data)
        issue_data["all_sprints"] = jira_report.get_all_sprint_data(sprint_data)
        issue_data["key"] = issue.key
        rows.append(issue_data)
    return rows


def pull_data(query):
    """Pulls all issues for open and completed sprints"""
    query_filter = PROJECT_QUERIES[query]["filter"]

    logger.debug(f"Report name: {query_name}")
    logger.debug(f"JQL filter: {query_filter}")

    jira_report = Report(query_filter)
    all_issues = jira_report.issues
    logger.info(f"Loaded {len(all_issues)} jira_reporting issues for {query}")
    rows = get_relevant_issue_data(jira_report)
    jira_data = list_to_df(rows)
    jira_data = jira_data.rename(columns={"last_sprint": "sprint"})
    insert_cols = list(jira_data.to_dict().keys())
    try:
        dataframe_to_db(
            jira_data, table_name="jira_issues", conflicts=["key"], cols=insert_cols
        )
    except Exception as err:
        logger.info(err)
        logger.warning(f"Failed to upload data to db")


if __name__ == "__main__":
    """Refresh Jira data for all configured projects and stores in db"""
    logger.info("Refreshing jira_reporting data for all projects")
    from util import PROJECT_QUERIES
    import os

    # filters_path = os.path.join(os.getcwd(), 'filters.txt')
    # PROJECT_QUERIES = get_jql_filters(filters_path)
    for query_name in PROJECT_QUERIES.keys():
        pull_data(query_name)
    logger.info("Data refresh complete.")
