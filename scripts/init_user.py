from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from tqdm import tqdm
from pathlib import Path
from argparse import ArgumentParser
import yaml
from addict import Dict

parser = ArgumentParser()
parser.add_argument("--config", "-c", type=str, default="config.yaml", help="Path to the configuration file")
args = parser.parse_args()

with open(args.config, "r") as f:
    config = Dict(yaml.safe_load(f))

url = config.gitlab.url
private_token = config.gitlab.token

headers = {
    'PRIVATE-TOKEN': private_token
}

# 查找用户
def find_user(username):
    response = requests.get(f"{url}/users?username={username}", headers=headers)
    assert response.status_code == 200, f"Failed to fetch user {username}: {response.status_code}, {response.text}"
    assert len(response.json()) > 0, f"User {username} not found."
    return response.json()[0]  # 返回用户信息

def find_project(project_path):
    response = requests.get(f"{url}/projects/{project_path.replace('/', '%2F')}", headers=headers)
    assert response.status_code == 200, f"Failed to fetch project {project_path}: {response.status_code}, {response.text}"
    return response.json()

def process_student(teacher, username, name):
    try:
        user = find_user(username)
        user_id = user["id"]
    except Exception as e:
        return f"{username},{name},Failed,Failed"
    try:
        project = find_project(f"{config.repo.group}/{teacher}/cp-{username}")
        project_id = project["id"]
    except Exception as e:
        return f"{username},{name},{user_id},Failed"
    return f"{username},{name},{user_id},{project_id}"

data_root = Path(config.data_root).resolve() / "students"
output_root = Path(config.data_root).resolve() / "repo"
output_root.mkdir(exist_ok=True)

classes = sorted(data_root.glob("*.csv"))
for class_file in classes:
    teacher, group_id = class_file.stem.split("-")
    print(teacher, group_id)
    result_file = output_root / f"{teacher}-{group_id}.csv"
    results = []
    with class_file.open() as f:
        lines = f.readlines()
    total = len(lines)
    failed = 0
    with ThreadPoolExecutor() as executor:
        future_to_index = {executor.submit(process_student, teacher, *line.strip().split(",")): i for i, line in enumerate(lines)}
        results_indexed = {}
        for future in tqdm(as_completed(future_to_index), total=total):
            i = future_to_index[future]
            results_indexed[i] = future.result()
        for i in range(total):
            username, name, user_id, project_id = results_indexed[i].split(",")
            if project_id == "Failed":
                failed += 1
            results.append(results_indexed[i])
    with result_file.open("w") as f:
        f.write("\n".join(results))
    print(f"Saved to {result_file}")
    print(f"Total: {total}, Failed: {failed}, {failed/total:.2%}")