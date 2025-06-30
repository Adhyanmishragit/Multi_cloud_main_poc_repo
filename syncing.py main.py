import os
import requests
import base64
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configuration from .env
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


def get_headers(token):
    """Generate request headers."""
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }


def list_notebooks(workspace_url, access_token, path="/"):
    """Fetch all notebook paths from the source workspace."""
    api_endpoint = f"{workspace_url}/api/2.0/workspace/list"
    try:
        response = requests.get(api_endpoint, headers=get_headers(access_token), params={"path": path})
        response.raise_for_status()
        objects = response.json().get("objects", [])

        # Collect all notebook paths
        notebooks = []
        for obj in objects:
            obj_path = obj["path"]
            if obj.get("object_type") == "NOTEBOOK":
                notebooks.append(obj_path)
            elif obj.get("object_type") == "DIRECTORY":
                # Recursively fetch notebooks inside directories
                notebooks.extend(list_notebooks(workspace_url, access_token, obj_path))
        
        return notebooks
    except requests.exceptions.RequestException as e:
        print(f"Error fetching notebooks from {workspace_url}: {e}")
        return []


def export_notebook(workspace_url, access_token, path):
    """Export a notebook from a workspace."""
    api_endpoint = f"{workspace_url}/api/2.0/workspace/export"
    try:
        response = requests.get(api_endpoint, headers=get_headers(access_token), params={"path": path, "format": "SOURCE"})
        response.raise_for_status()
        return base64.b64decode(response.json()["content"]).decode("utf-8")
    except requests.exceptions.RequestException as e:
        print(f"Error exporting notebook {path} from {workspace_url}: {e}")
        return None


def import_notebook(workspace_url, access_token, content, path, language='PYTHON'):
    """Import a notebook into a workspace."""
    existing_content = export_notebook(workspace_url, access_token, path)
    if existing_content and existing_content == content:
        print(f"Notebook {path} already up-to-date in {workspace_url}")
        return

    api_endpoint = f"{workspace_url}/api/2.0/workspace/import"
    data = {
        "path": path,
        "format": "SOURCE",
        "language": language,
        "content": base64.b64encode(content.encode("utf-8")).decode("utf-8"),
        "overwrite": True
    }
    try:
        response = requests.post(api_endpoint, headers=get_headers(access_token), json=data)
        response.raise_for_status()
        print(f"Notebook imported successfully to {path} in {workspace_url}")
    except requests.exceptions.RequestException as e:
        print(f"Error importing notebook to {path} in {workspace_url}: {e}")


def sync_notebooks(source_cloud, target_cloud):
    """Sync notebooks between two workspaces."""
    print(f"Starting notebook synchronization from {source_cloud} to {target_cloud}...")

    # Get workspace configurations
    source_config = WORKSPACE_CONFIG.get(source_cloud.upper())
    target_config = WORKSPACE_CONFIG.get(target_cloud.upper())

    if not source_config or not target_config:
        print(f"Invalid source or target cloud provider: {source_cloud} -> {target_cloud}")
        return

    # Fetch all notebooks dynamically from the source workspace
    notebook_paths = list_notebooks(source_config["url"], source_config["token"])

    if not notebook_paths:
        print(f"No notebooks found in {source_cloud} workspace.")
        return

    # Sync notebooks
    for notebook_path in notebook_paths:
        content = export_notebook(source_config["url"], source_config["token"], notebook_path)
        if content:
            import_notebook(target_config["url"], target_config["token"], content, notebook_path)

    print(f"Notebook synchronization from {source_cloud} to {target_cloud} completed.")


if __name__ == "__main__":
    # Sync from GCP to Azure
    sync_notebooks("GCP", "AZURE")

    # Sync from Azure to GCP
    sync_notebooks("AZURE", "GCP")