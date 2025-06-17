from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
import requests
from tqdm import tqdm
from argparse import ArgumentParser
import yaml
from addict import Dict

parser = ArgumentParser()
parser.add_argument("branch", type=str, help="The branch name to get reports from")
parser.add_argument("teacher", type=str, nargs='?', default=None, help="The teacher's name to filter reports")
parser.add_argument("--config", "-c", type=str, default="config.yaml", help="Path to the configuration file")
args = parser.parse_args()

with open(args.config, "r") as f:
    config = Dict(yaml.safe_load(f))

GITLAB_URL = config.gitlab.url
PRIVATE_TOKEN = config.gitlab.token

BRANCH = args.branch
TEACHER = args.teacher

# 获取仓库分支的最新 commit id
def get_latest_commit_id(project_id, branch):
    url = f"{GITLAB_URL}/projects/{project_id}/repository/branches/{branch}"
    headers = {"PRIVATE-TOKEN": PRIVATE_TOKEN}
    
    response = requests.get(url, headers=headers)
    assert response.status_code == 200, f"Failed to get branch information: {response.status_code}"
    branch_info = response.json()
    return branch_info['commit']['id']

def get_file_info(project_id, file_path, ref):
    url = f"{GITLAB_URL}/projects/{project_id}/repository/files/{requests.utils.quote(file_path, safe='')}"
    headers = {"PRIVATE-TOKEN": PRIVATE_TOKEN}
    params = {"ref": ref}
    
    response = requests.get(url, headers=headers, params=params)
    assert response.status_code == 200, f"Failed to get file information: {response.status_code}"
    return response.json()

def get_raw_file(project_id, file_path, ref, save_path: Path):
    url = f"{GITLAB_URL}/projects/{project_id}/repository/files/{requests.utils.quote(file_path, safe='')}/raw"
    headers = {"PRIVATE-TOKEN": PRIVATE_TOKEN}
    params = {"ref": ref}
    
    response = requests.get(url, headers=headers, params=params)
    assert response.status_code == 200, f"Failed to get file information: {response.status_code}"
    
    save_path.parent.mkdir(exist_ok=True, parents=True)
    with open(save_path, "wb") as f:
        f.write(response.content)

def get_report(username, name, user_id, project_id):
    commit_id = get_latest_commit_id(project_id, BRANCH)
    get_raw_file(project_id, f'reports/{BRANCH}.pdf', commit_id, output_root / f"{username}-{name}.pdf")

def process_student(username, name, user_id, project_id):
    if project_id == "Failed":
        # return
        print(f"Project not found for {username} {name}")
    try:
        get_report(username, name, user_id, project_id)
    except Exception as e:
        # return
        print(f"Failed to get report for {username}: {e}")
        
data_root = Path(config.data_root).resolve() / "repo"

classes = sorted(data_root.glob("*.csv"))
for class_file in classes:
    teacher, group_id = class_file.stem.split("-")
    if TEACHER and TEACHER != teacher:
        continue
    output_root = Path(config.data_root).resolve() / "reports" / BRANCH / teacher
    output_root.mkdir(exist_ok=True, parents=True)
    print(teacher, group_id)
    with class_file.open() as f:
        lines = f.readlines()
    total = len(lines)
    with ThreadPoolExecutor() as executor:
        future_to_index = {executor.submit(process_student, *line.strip().split(",")): i for i, line in enumerate(lines)}
        for future in tqdm(as_completed(future_to_index), total=total):
            i = future_to_index[future]
            future.result()