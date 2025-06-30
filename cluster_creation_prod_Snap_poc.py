import requests
import json

# Environment configurations
ENVIRONMENTS = {
    "staging": {
        "url": "https://adb-2709448595204364.4.azuredatabricks.net/",
        "token": "dapid9ac15f2e469fd5e2ca00665cdee5592"
    },
    "preprod": {
        "url": "https://adb-3178080356403137.17.azuredatabricks.net/",
        "token": "dapi7c67f37deb934534b90c259a342ac272"
    }
}

# Function to select environment
def select_environment():
    print("Select the source environment:")
    for env in ENVIRONMENTS.keys():
        print(f"- {env}")
    source_env = input("Enter source environment: ").strip().lower()
    
    print("Select the target environment:")
    for env in ENVIRONMENTS.keys():
        print(f"- {env}")
    target_env = input("Enter target environment: ").strip().lower()
    
    if source_env not in ENVIRONMENTS or target_env not in ENVIRONMENTS:
        print("Invalid environment selected. Exiting.")
        exit(1)
    
    return ENVIRONMENTS[source_env], ENVIRONMENTS[target_env]

# Step 1: Export Clusters
def export_clusters(source_config):
    print("Exporting clusters...")
    clusters_url = f"{source_config['url']}/api/2.0/clusters/list"
    headers = {
        "Authorization": f"Bearer {source_config['token']}",
        "Content-Type": "application/json"
    }
    response = requests.get(clusters_url, headers=headers)
    if response.status_code == 200:
        clusters = response.json().get("clusters", [])
        with open("clusters.json", "w") as f:
            json.dump(clusters, f, indent=4)
        print(f"Exported {len(clusters)} clusters to 'clusters.json'.")
    else:
        print(f"Failed to export clusters: {response.status_code}, {response.text}")
        exit(1)

# Step 2: Recreate Clusters in Target Workspace
def recreate_clusters(target_config):
    print("Recreating clusters...")
    with open("clusters.json", "r") as f:
        clusters = json.load(f)
    create_cluster_url = f"{target_config['url']}/api/2.0/clusters/create"
    headers = {
        "Authorization": f"Bearer {target_config['token']}",
        "Content-Type": "application/json"
    }
    for cluster in clusters:
        cluster_config = cluster.copy()
        # Remove fields that cannot be set during creation
        cluster_config.pop("cluster_id", None)
        cluster_config.pop("state", None)
        cluster_config.pop("default_tags", None)
        response = requests.post(create_cluster_url, headers=headers, json=cluster_config)
        if response.status_code == 200:
            print(f"Cluster '{cluster_config.get('cluster_name')}' created successfully.")
        else:
            print(f"Failed to create cluster '{cluster_config.get('cluster_name')}': {response.status_code}, {response.text}")

# Main function
def main():
    # Select source and target environments
    source_config, target_config = select_environment()
    
    # Export clusters from source environment
    export_clusters(source_config)
    
    # Recreate clusters in target environment
    recreate_clusters(target_config)

if __name__ == "__main__":
    main()