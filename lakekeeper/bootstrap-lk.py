import os
import time
import requests

S3_ACCESS_KEY = os.environ.get("S3_LAKEKEEPER_USER", "lakekeeper")
S3_SECRET_KEY = os.environ.get("S3_LAKEKEEPER_PASSWORD", "LKpassw0rd!")


def init_lk():
    # boostrap
    print("Starting Lakekeeper bootstrap process...")

    # Add retries for the bootstrap request
    for _ in range(3):
        resp = requests.post(
            url="http://lakekeeper:8181/management/v1/bootstrap",
            json={"accept-terms-of-use": True},
        )
        print(f"Bootstrap response status code: {resp.status_code}")
        if resp.status_code in [200, 400]:
            print("Bootstrap process completed successfully.")
            break
        time.sleep(5)
    else:
        print("Bootstrap process failed.")
        resp.raise_for_status()

    # create warehouse
    resp = requests.post(
        url="http://lakekeeper:8181/management/v1/warehouse",
        json={
            "warehouse-name": "DataLake",
            "project-id": "00000000-0000-0000-0000-000000000000",
            "storage-profile": {
                "type": "s3",
                "bucket": "warehouse",
                "key-prefix": "datalake-warehouse",
                "endpoint": "http://minio:9000",
                "region": "local",
                "path-style-access": True,
                "flavor": "s3-compat",
                "sts-enabled": True,
            },
            "storage-credential": {
                "type": "s3",
                "credential-type": "access-key",
                "aws-access-key-id": S3_ACCESS_KEY,
                "aws-secret-access-key": S3_SECRET_KEY,
            },
        },
    )

    print(f"Create warehouse status code: {resp.status_code}")
    if resp.status_code not in [400, 409, 200, 201]:
        print("Error creating warehouse, raising exception.")
        print(resp.text)
        resp.raise_for_status()
    else:
        if resp.status_code in [400, 409]:
            print("Warehouse already exists, skipping creation.")
        else:
            print("Warehouse created successfully.")

    # add namespace
    print("Creating namespace for DataLake...")
    warehouse_prefix = requests.get(
        "http://lakekeeper:8181/catalog/v1/config?warehouse=DataLake"
    ).json()["defaults"]["prefix"]
    print(warehouse_prefix)
    
    ns_resp = requests.post(
        f"http://lakekeeper:8181/catalog/v1/{warehouse_prefix}/namespaces",
        json={"namespace": ["DataLake"]},
    )
    print(f"Create namespace status code: {ns_resp.status_code}")
    if ns_resp.status_code not in [409, 200]:
        print("Error creating namespace, raising exception.")
        ns_resp.raise_for_status()
    else:
        if ns_resp.status_code == 409:
            print("Namespace already exists, skipping creation.")
        else:
            print("Namespace created successfully.")


if __name__ == "__main__":
    init_lk()
