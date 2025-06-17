from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
import requests
from tqdm import tqdm
from argparse import ArgumentParser
import yaml
from addict import Dict

parser = ArgumentParser()
parser.add_argument("branch", type=str, help="The branch name to set as protected")
parser.add_argument("--config", "-c", type=str, default="config.yaml", help="Path to the configuration file")
args = parser.parse_args()

with open(args.config, "r") as f:
    config = Dict(yaml.safe_load(f))

GITLAB_URL = config.gitlab.url
ACCESS_TOKEN = config.gitlab.token

BRANCH_NAME = args.branch

HEADERS = {
    "Private-Token": ACCESS_TOKEN
}

def get_group_projects(group_id):
    """获取 group 下的所有项目"""
    url = f"{GITLAB_URL}/groups/{group_id}/projects"
    projects = []
    page = 1

    while True:
        response = requests.get(url, headers=HEADERS, params={"per_page": 100, "page": page})
        if response.status_code != 200:
            print(f"获取项目列表失败: {response.json()}")
            return []
        
        data = response.json()
        if not data:
            break
        
        projects.extend(data)
        page += 1

    return projects

def set_protected_branch(project_id, branch):
    """设置指定项目的 protected branch 规则为 no one 可 push/merge"""
    url = f"{GITLAB_URL}/projects/{project_id}/protected_branches/{branch}"

    # 先检查该分支是否已被保护
    response = requests.get(url, headers=HEADERS)
    
    if response.status_code == 200:
        # 如果分支已受保护，先解除保护
        response = requests.delete(url, headers=HEADERS)
        assert response.status_code == 204, f"解除分支保护失败: {response.json() if response.content else 'No content'}"
    
    # 创建新的保护分支
    create_url = f"{GITLAB_URL}/projects/{project_id}/protected_branches"
    response = requests.post(create_url, headers=HEADERS, json={
        "name": branch,
        "push_access_level": 0,
        "merge_access_level": 0
    })
    
    assert response.status_code == 201, f"创建受保护分支失败: {response.json()}"

def process_student(username, name, user_id, project_id):
    if project_id == "Failed":
        print(f"Failed to find project for {username}")
    try:
        set_protected_branch(project_id, BRANCH_NAME)
    except Exception as e:
        print(f"Failed to set protected branch for cp-{username} {name}: {e}")


data_root = Path(config.data_root).resolve() / "repo"

classes = sorted(data_root.glob("*.csv"))
for class_file in classes:
    teacher, group_id = class_file.stem.split("-")
    print(teacher, group_id)
    results = []
    with class_file.open() as f:
        lines = f.readlines()
    total = len(lines)
    failed = 0
    with ThreadPoolExecutor() as executor:
        future_to_index = {executor.submit(process_student, *line.strip().split(",")): i for i, line in enumerate(lines)}
        results_indexed = {}
        for future in tqdm(as_completed(future_to_index), total=total):
            i = future_to_index[future]
            results_indexed[i] = future.result()
    print(f"Total: {total}, Failed: {failed} ({failed/total:.2%})")