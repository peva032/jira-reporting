from core import ProjectData, get_project_keys, get_jira_connection


def refresh_sprint_data():
    print('---' * 40)
    jira_connection = get_jira_connection()
    project_keys = get_project_keys(jira_connection=jira_connection)
    for project_key in project_keys:
        project_data = ProjectData(jira_connection=jira_connection, project_key=project_key)
        project_data.refresh_sprint_data()
        print('---' * 40)


if __name__ == '__main__':
    refresh_sprint_data()
