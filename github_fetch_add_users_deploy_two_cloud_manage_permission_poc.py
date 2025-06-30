import os
import requests
import base64
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configuration
WORKSPACE_CONFIG = {
    "AWS": {
        "url": os.getenv("AWS_WORKSPACE_URL"),
        "token": os.getenv("AWS_ACCESS_TOKEN"),
    },
    "AZURE": {
        "url": os.getenv("AZURE_WORKSPACE_URL"),
        "token": os.getenv("AZURE_ACCESS_TOKEN"),
    },
    "GCP": {
        "url": os.getenv("GCP_WORKSPACE_URL"),
        "token": os.getenv("GCP_ACCESS_TOKEN"),
    },
}

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
GITHUB_REPO_OWNER = os.getenv("GITHUB_REPO_OWNER")
GITHUB_REPO_NAME = os.getenv("GITHUB_REPO_NAME")

# Load users and their permissions from .env
USERS = os.getenv("USERS")
if USERS:
    USERS = {
        user: permission_level
        for user, permission_level in [
            perm.split(":") for perm in USERS.split(",")
        ]
    }
else:
    USERS = {
        "somin.sangwan@digivatelabs.com": "CAN_MANAGE",
        "samir.shinde@digivatelabs.com": "CAN_MANAGE"
    }


def get_headers(token):
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }


def get_workspace_config(cloud_provider):
    return WORKSPACE_CONFIG.get(cloud_provider.upper(), None)


def fetch_notebook_from_github(repo_owner, repo_name, notebook_path, github_token):
    """
    Fetch a notebook from GitHub using the GitHub API.
    """
    api_endpoint = f"https://api.github.com/repos/{repo_owner}/{repo_name}/contents/{notebook_path}"
    headers = {
        "Authorization": f"token {github_token}",
        "Accept": "application/vnd.github.v3+json"
    }
    try:
        response = requests.get(api_endpoint, headers=headers)
        response.raise_for_status()
        content = response.json().get("content")
        if content:
            # Decode base64 content
            return base64.b64decode(content).decode("utf-8")
        else:
            print(f"Notebook {notebook_path} not found in GitHub repository.")
            return None
    except requests.exceptions.RequestException as e:
        print(f"Error fetching notebook {notebook_path} from GitHub: {e}")
        return None


def fetch_all_files_from_github(repo_owner, repo_name, github_token, path=""):
    """
    Fetch all files from a GitHub repository or a specific directory.
    """
    api_endpoint = f"https://api.github.com/repos/{repo_owner}/{repo_name}/contents/{path}"
    headers = {
        "Authorization": f"token {github_token}",
        "Accept": "application/vnd.github.v3+json"
    }
    try:
        response = requests.get(api_endpoint, headers=headers)
        response.raise_for_status()
        files = response.json()
        
        file_contents = {}
        for file in files:
            if file["type"] == "file":  # Fetch all files, not just .py files
                file_content = fetch_notebook_from_github(repo_owner, repo_name, file["path"], github_token)
                if file_content:
                    file_contents[file["name"]] = file_content
            elif file["type"] == "dir":  # Recursively fetch files from subdirectories
                subdir_files = fetch_all_files_from_github(repo_owner, repo_name, github_token, file["path"])
                file_contents.update(subdir_files)
        
        return file_contents
    except requests.exceptions.RequestException as e:
        print(f"Error fetching files from GitHub repository: {e}")
        return {}


def import_notebook(workspace_url, access_token, content, notebook_name, workspace_dir):
    """
    Import a notebook into a Databricks workspace.
    """
    api_endpoint = f"{workspace_url}/api/2.0/workspace/import"
    notebook_path = f"{workspace_dir}/{notebook_name}"
    data = {
        "path": notebook_path,
        "format": "SOURCE",
        "language": "PYTHON",
        "content": base64.b64encode(content.encode("utf-8")).decode("utf-8"),
        "overwrite": True
    }
    try:
        response = requests.post(api_endpoint, headers=get_headers(access_token), json=data)
        response.raise_for_status()
        print(f"Notebook imported successfully to {notebook_path} in {workspace_url}")
        return notebook_path
    except requests.exceptions.RequestException as e:
        print(f"Error importing notebook to {notebook_path} in {workspace_url}: {e}")
        return None


def get_object_status(workspace_url, access_token, path):
    """
    Fetch the status of an object (notebook or directory) in a Databricks workspace.
    """
    api_endpoint = f"{workspace_url}/api/2.0/workspace/get-status"
    params = {"path": path}
    try:
        response = requests.get(api_endpoint, headers=get_headers(access_token), params=params)
        response.raise_for_status()
        print(f"Object status for {path}: {response.json()}")  # Debugging log
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching object status for {path} in {workspace_url}: {e}")
        return None


def add_user_to_workspace(workspace_url, access_token, email):
    """
    Add a user to the Databricks workspace.
    """
    api_endpoint = f"{workspace_url}/api/2.0/preview/scim/v2/Users"
    data = {
        "schemas": ["urn:ietf:params:scim:schemas:core:2.0:User"],
        "userName": email,
        "entitlements": [
            {"value": "allow-cluster-create"}
        ]
    }
    try:
        response = requests.post(api_endpoint, headers=get_headers(access_token), json=data)
        response.raise_for_status()
        print(f"User {email} added to workspace successfully.")
        return True
    except requests.exceptions.RequestException as e:
        print(f"Error adding user {email} to workspace: {e}")
        return False


def check_user_exists(workspace_url, access_token, email):
    """
    Check if a user exists in the Databricks workspace.
    """
    api_endpoint = f"{workspace_url}/api/2.0/preview/scim/v2/Users"
    params = {"filter": f"userName eq '{email}'"}
    try:
        response = requests.get(api_endpoint, headers=get_headers(access_token), params=params)
        response.raise_for_status()
        users = response.json().get("Resources", [])
        return len(users) > 0
    except requests.exceptions.RequestException as e:
        print(f"Error checking if user {email} exists: {e}")
        return False


def create_directory(workspace_url, access_token, path):
    """
    Create a directory in the Databricks workspace.
    """
    api_endpoint = f"{workspace_url}/api/2.0/workspace/mkdirs"
    data = {"path": path}
    try:
        response = requests.post(api_endpoint, headers=get_headers(access_token), json=data)
        response.raise_for_status()
        print(f"Directory created successfully: {path}")
        return True
    except requests.exceptions.RequestException as e:
        print(f"Error creating directory {path}: {e}")
        return False


def grant_permissions(workspace_url, access_token, path, user, permission_level, cluster_id):
    """
    Grant permissions to a user for a specific path in the Databricks workspace.
    """
    api_endpoint = f"{workspace_url}/api/2.0/permissions/directories/{path}"
    data = {
        "access_control_list": [
            {
                "user_name": user,
                "permission_level": permission_level
            }
        ]
    }
    try:
        response = requests.patch(api_endpoint, headers=get_headers(access_token), json=data)
        response.raise_for_status()
        print(f"Permissions granted to {user} for {path} successfully.")
        return True
    except requests.exceptions.RequestException as e:
        print(f"Error granting permissions to {user} for {path}: {e}")
        return False


def deploy_to_workspace(workspace_url, access_token, all_files, workspace_dir):
    """
    Deploy all files to a specific workspace.
    """
    for user, permission_level in USERS.items():
        print(f"Processing user: {user}")

        # Check if the user exists in the workspace
        if not check_user_exists(workspace_url, access_token, user):
            print(f"User {user} does not exist in the workspace. Adding user...")
            if not add_user_to_workspace(workspace_url, access_token, user):
                print(f"Failed to add user {user}. Skipping.")
                continue

        # Create a directory for the user if it doesn't exist
        user_directory = f"{workspace_dir}/{user}"
        if not create_directory(workspace_url, access_token, user_directory):
            print(f"Failed to create directory for user {user}. Skipping.")
            continue

        # Import all files into the user's directory in the workspace
        print(f"Importing files to {user_directory} in workspace...")
        for file_name, file_content in all_files.items():
            target_file_path = import_notebook(workspace_url, access_token, file_content, file_name, user_directory)
            if not target_file_path:
                print(f"Failed to import {file_name} into {user_directory}.")

        # Grant permissions to the user for their directory
        print(f"Granting permissions to {user} for {user_directory}...")
        grant_permissions(workspace_url, access_token, user_directory, user, permission_level, None)


def sync_to_source_workspace(source_cloud, git_url=None, cluster_id=None):
    print("Starting notebook synchronization and permission sync for source workspace...")

    # Fetch all files from the GitHub repository
    print("Fetching all files from GitHub repository...")
    all_files = fetch_all_files_from_github(GITHUB_REPO_OWNER, GITHUB_REPO_NAME, GITHUB_TOKEN)
    if not all_files:
        print("Failed to fetch files from GitHub repository. Exiting.")
        return

    # Get source workspace configuration
    source_config = get_workspace_config(source_cloud)
    if not source_config:
        print("Invalid source cloud provider.")
        return

    # Deploy to source workspace
    print(f"Deploying to source workspace: {source_cloud}")
    deploy_to_workspace(source_config["url"], source_config["token"], all_files, "/Users")

    print("Notebook synchronization and permission sync completed for source workspace.")


def sync_to_destination_workspace(target_cloud, git_url=None, cluster_id=None):
    print("Starting synchronization to destination workspace...")

    # Fetch all files from the GitHub repository
    print("Fetching all files from GitHub repository...")
    all_files = fetch_all_files_from_github(GITHUB_REPO_OWNER, GITHUB_REPO_NAME, GITHUB_TOKEN)
    if not all_files:
        print("Failed to fetch files from GitHub repository. Exiting.")
        return

    # Get target workspace configuration
    target_config = get_workspace_config(target_cloud)
    if not target_config:
        print("Invalid target cloud provider.")
        return

    # Deploy to target workspace
    print(f"Deploying to target workspace: {target_cloud}")
    deploy_to_workspace(target_config["url"], target_config["token"], all_files, "/Users")

    print("Notebook synchronization and permission sync completed for destination workspace.")


if __name__ == "__main__":
    # Step 1: Ask for source workspace and sync
    source_cloud = input("Enter source cloud provider (AWS/AZURE/GCP): ")
    git_url = input("Enter Git repository URL (optional): ")
    cluster_id = input("Enter cluster ID for attach permissions (optional): ")

    sync_to_source_workspace(source_cloud, git_url, cluster_id)

    # Step 2: Ask for destination workspace and sync
    target_cloud = input("Enter target cloud provider (AWS/AZURE/GCP): ")
    sync_to_destination_workspace(target_cloud, git_url, cluster_id)