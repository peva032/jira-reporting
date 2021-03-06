import os
import re

import pandas as pd
from donfig import Config
from jira import JIRA
from loguru import logger

config = Config("jira_reporting")


class Report:
    def __init__(self, jql_filter: dict):
        self.jira_connection = self.get_jira_connection()
        self.issues = self.load_jira_issue(filt=jql_filter)

    @staticmethod
    def get_jira_connection():
        user = config.get("USER", None) or os.environ.get('JIRA_USER', None)
        token = config.get("TOKEN", None) or os.environ.get('JIRA_TOKEN', None)
        server = config.get("SERVER", None) or os.environ.get('JIRA_SERVER', None)
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
                    extracted_issue_data[field_name] = None if str(val).lower() == 'nan' else val
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


__all__ = ["Report", "config", "list_to_df"]
