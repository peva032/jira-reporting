import os
import re
from datetime import datetime

import numpy as np
import pandas as pd
import pytz
from donfig import Config
from jira import JIRA
from loguru import logger

from db import dataframe_to_db
from tqdm import tqdm

config = Config("jira_reporting")


class Report:
    def __init__(self, jql_filter: dict):
        self.jira_connection = self.get_jira_connection()
        self.issues = self.load_jira_issue(filt=jql_filter)

    @staticmethod
    def get_jira_connection():
        user = config.get("USER", None) or os.environ.get("JIRA_USER", None)
        token = config.get("TOKEN", None) or os.environ.get("JIRA_TOKEN", None)
        server = config.get("SERVER", None) or os.environ.get("JIRA_SERVER", None)
        options = {"server": server}
        return JIRA(options=options, basic_auth=(user, token))

    def load_jira_issue(self, filt):
        return self.jira_connection.search_issues(filt, maxResults=1000)

    @staticmethod
    def get_issue_field_val(issue, field_name, field_data_key=None):
        """Tries to find field in issue and returns value"""
        issue_fields = [field for field in issue.raw["fields"].keys()]
        assert field_name in issue_fields
        if field_data_key:
            return issue.raw["fields"][field_name][field_data_key]
        return issue.raw["fields"][field_name]

    def get_issue_data(self, issue, fields: dict) -> dict:
        extracted_issue_data = {}
        for field_name, field_key in fields.items():
            try:
                val = self.get_issue_field_val(issue, field_name, field_key)
                if field_name == "resolution":
                    extracted_issue_data[field_name] = 1
                else:
                    extracted_issue_data[field_name] = (
                        None if str(val).lower() == "nan" else val
                    )
            except TypeError:
                if field_name == "resolution":
                    extracted_issue_data[field_name] = 0
                else:
                    extracted_issue_data[field_name] = None
        return extracted_issue_data

    @staticmethod
    def get_last_sprint(sprint_data: list) -> str:
        """Looks for latest sprint label"""
        sequences = [sprint_val.get("id") for sprint_val in sprint_data]
        sprint_names = [sprint_val.get("name", None) for sprint_val in sprint_data]
        return sprint_names[sequences.index(max(sequences))]

    @staticmethod
    def get_all_sprint_data(sprint_data: list) -> list:
        sprint_names = [sprint_val.get("name", None) for sprint_val in sprint_data]
        return ", ".join([str(z) for z in sprint_names])

    def get_sprint_counts(self, issues, sprint_col) -> dict:
        sprint_data = [self.get_issue_field_val(issue, sprint_col) for issue in issues]
        all_sprint_data = sum(sprint_data, [])
        sprint_names = [
            re.findall(r"name=[^,]*", str(sprint_val))[0].replace("name=", "")
            for sprint_val in all_sprint_data
        ]
        return {k: sprint_names.count(k) for k in set(sprint_names)}

    @staticmethod
    def get_issues_completed(jira_data):
        assert "sprint" in jira_data.columns
        completed_cards = list(
            jira_data[jira_data["status"].str.lower() == "done"]["sprint"]
        )
        return {k: completed_cards.count(k) for k in set(completed_cards)}

    @staticmethod
    def sprint_summary(sprint_counts, completed_issues):
        sprint_data = {"sprint": [], "completed": [], "total": []}
        for k, v in sprint_counts.items():
            sprint_data["sprint"].append(k)
            sprint_data["total"].append(v)
            try:
                sprint_data["completed"].append(completed_issues[k])
            except KeyError:
                sprint_data["completed"].append(0)
        return pd.DataFrame(sprint_data).sort_values("sprint")


class ProjectData:
    def __init__(self, jira_connection: JIRA, project_key: str):
        self.jira_connection = jira_connection
        self.project_key = project_key
        self.issues = self.get_project_issues()
        self.populated_issues = []
        self.all_issue_data = []
        self.all_sprint_data = []
        self.project_data = pd.DataFrame()

    def get_project_issues(self) -> list:
        """
        Makes jira JQL search for project and issues types.
        Gets latest 100 (maximum)
        """
        logger.info(f"Fetching issues for {self.project_key}")
        query = (
            f"project={self.project_key} "
            + f"and issuetype in (Task, Bug, Subtask, Sub-task) "
            + f"ORDER BY updated DESC"
        )
        return self.jira_connection.search_issues(query, maxResults=20)

    def get_issues_from_search_results(self) -> None:
        """
         Search results don't contain all data needed,
        so we need to convert to JIRA.issue objects using
        the issue ids
        """
        logger.info(f"Collecting data for {len(self.issues)} issues")
        with tqdm(total=len(self.issues)) as pbar:
            for iss in self.issues:
                issue_with_data = self.jira_connection.issue(iss.id)
                # Filter issues that don't have a sprint assigned to them
                if issue_with_data.fields.customfield_10020:
                    self.populated_issues.append(issue_with_data)
                pbar.update(1)

    @staticmethod
    def get_issue_sprint_data(iss: JIRA.issue) -> list:
        issue_sprints = iss.fields.customfield_10020
        return [
            {
                "sprint_name": sprint.name,
                "start_date": sprint.startDate,
                "end_date": sprint.endDate,
                "board_id": sprint.boardId,
                "sprint_state": sprint.state,
                "sprint_number": float(sprint.name.split(" ")[-1]),
            }
            for sprint in issue_sprints
        ]

    @staticmethod
    def extract_issue_data(iss: JIRA.issue) -> dict:
        """Extract key issue field data"""
        return {
            "key": iss.key,
            "id": iss.id,
            "project": iss.fields.project.key,
            "issue_type": (iss.fields.issuetype.name if iss.fields.issuetype else None),
            "summary": iss.fields.summary,
            "assignee": (
                iss.fields.assignee.displayName if iss.fields.assignee else None
            ),
            "reporter": (
                iss.fields.reporter.displayName if iss.fields.reporter else None
            ),
            "priority": (iss.fields.priority.name if iss.fields.priority else None),
            "status": (iss.fields.status.name if iss.fields.status else None),
            "resolution": (
                iss.fields.resolution.name if iss.fields.resolution else None
            ),
            "resolved": (
                1
                if iss.fields.resolution
                and iss.fields.resolution.name in ("Done", "DONE")
                else 0
            ),
            "created": iss.fields.created,
            "updated": iss.fields.updated,
            "due_date": iss.fields.duedate,
            "total_time_spent": iss.fields.timespent,
            "total_time_estimate": iss.fields.timeestimate,
            "original_time_estimate": iss.fields.timeoriginalestimate,
            "remaining_time_estimate": (
                iss.fields.timetracking.remainingEstimateSeconds
                if iss.fields.timetracking.raw
                else None
            ),
        }

    @staticmethod
    def get_issue_worklogs(iss: JIRA.issue) -> list:
        issue_worklogs = iss.fields.worklog.worklogs
        return [
            {
                "time_spent": wl.timeSpentSeconds,
                "started": wl.started,
                "updated": wl.updated,
                "worklog_author": wl.author.displayName,
            }
            for wl in issue_worklogs
        ]

    @staticmethod
    def worklog_within_sprint(worklog: dict, sprint_data: dict) -> bool:
        utc = pytz.utc
        worklog_started = datetime.strptime(
            worklog.get("started"), "%Y-%m-%dT%H:%M:%S.%f%z"
        )
        sprint_start_date = datetime.strptime(
            sprint_data.get("start_date"), "%Y-%m-%dT%H:%M:%S.%fZ"
        ).replace(tzinfo=utc)
        sprint_end_date = datetime.strptime(
            sprint_data.get("end_date"), "%Y-%m-%dT%H:%M:%S.%fZ"
        ).replace(tzinfo=utc)
        return sprint_start_date <= worklog_started <= sprint_end_date

    def get_sprint_time_spent(self, iss: JIRA.issue, sprint_data: dict) -> dict:
        issue_worklogs = self.get_issue_worklogs(iss)
        sprint_time = 0
        for worklog in issue_worklogs:
            if self.worklog_within_sprint(worklog, sprint_data):
                sprint_time += worklog.get("time_spent", 0)
        return {"issue_key": iss.key, **sprint_data, "sprint_time_spent": sprint_time}

    def get_issue_sprint_data_with_time_spent(self, iss: JIRA.issue) -> list:
        issue_sprint_data = self.get_issue_sprint_data(iss)
        return [self.get_sprint_time_spent(iss, sd) for sd in issue_sprint_data]

    def get_issue_and_sprint_data(self) -> None:
        issues = self.populated_issues
        all_issue_level_data = [self.extract_issue_data(_issue) for _issue in issues]
        all_sprint_level_data = [
            self.get_issue_sprint_data_with_time_spent(_issue) for _issue in issues
        ]
        self.all_issue_data = all_issue_level_data
        self.all_sprint_data = sum(all_sprint_level_data, [])

    def merge_issue_and_sprint_data(self):
        issue_df = pd.DataFrame(self.all_issue_data)
        sprint_df = pd.DataFrame(self.all_sprint_data)
        if len(issue_df) > 0 and len(sprint_df) > 0:
            project_data = sprint_df.merge(issue_df, left_on="issue_key", right_on="key")
            project_data.drop("issue_key", inplace=True, axis=1)
            project_data.rename({"id": "issue_id"}, inplace=True, axis=1)
            project_data[
                [
                    "total_time_spent",
                    "total_time_estimate",
                    "original_time_estimate",
                    "remaining_time_estimate",
                ]
            ] = (
                project_data[
                    [
                        "total_time_spent",
                        "total_time_estimate",
                        "original_time_estimate",
                        "remaining_time_estimate",
                    ]
                ]
                .copy()
                .replace(np.nan, 0)
            )
            self.project_data = project_data


    def save_to_db(self):
        if len(self.project_data) > 0:
            cols = list(self.project_data.columns)
            conflicts = ("key", "sprint_name")
            dataframe_to_db(
                data=self.project_data,
                table_name=config.get("DB_TABLE_NAME", None) or os.environ.get("DB_TABLE_NAME"),
                conflicts=conflicts,
                cols=cols,
            )
        else:
            logger.info(f"No new data for {self.project_key}")

    def refresh_sprint_data(self):
        logger.info(f"Refreshing sprint data for {self.project_key}")
        self.get_issues_from_search_results()
        self.get_issue_and_sprint_data()
        self.merge_issue_and_sprint_data()
        self.save_to_db()


def list_to_df(dict_list: list) -> pd.DataFrame:
    if dict_list:
        d = {key: [] for key in dict_list[0].keys()}
        for row in dict_list:
            for k, v in row.items():
                d[k].append(v)
        return pd.DataFrame(d)
    else:
        logger.warning("No data found for this query")
        raise Exception("No data found for this query")


def get_jira_connection():
    user = config.get("JIRA_USER", None) or os.environ.get("JIRA_USER", None)
    token = config.get("JIRA_TOKEN", None) or os.environ.get("JIRA_TOKEN", None)
    server = config.get("JIRA_SERVER", None) or os.environ.get("JIRA_SERVER", None)
    options = {"server": server}
    return JIRA(options=options, basic_auth=(user, token))


def get_project_keys(jira_connection: JIRA) -> list:
    projects = jira_connection.projects()
    logger.info(f"Found {len(projects)} projects for {jira_connection.client_info()}")
    return [project.key for project in projects]


__all__ = [
    "Report",
    "config",
    "list_to_df",
    "get_project_keys",
    "ProjectData",
    "get_jira_connection",
]
