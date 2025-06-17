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


# 创建项目(通过fork)
# def create_project(username):
#     data = {
#         'name': 'cp-' + username,
#         'path': 'cp-' + username,
#         'namespace_id': 6630,
#         'visibility': 'private'
#     }
#     response = requests.post(f"{url}/projects/3299/fork", headers=headers, data=data)  # 45647是compiler/sp25-starter的项目ID
#     assert response.status_code == 201, f"Failed to fork project for {username}: {response.status_code}, {response.text}"
#     return response.json()['id']

def create_project(username, group_id):
    data = {
        'name': 'cp-' + username,
        'namespace_id': group_id,
        'visibility': 'private',
        'import_url': config.repo.import_url
    }
    response = requests.post(f"{url}/projects", headers=headers, data=data)
    assert response.status_code == 201, f"Failed to create project for {username}: {response.status_code}, {response.text}"
    return response.json()['id']

# 设置保护分支
def set_protected_branch(project_id, branch_pattern):
    data = {
        'name': branch_pattern,
        'push_access_level': 30,  # Developer
        'merge_access_level': 30,  # Developer
    }
    response = requests.post(f"{url}/projects/{project_id}/protected_branches", headers=headers, data=data)
    assert response.status_code == 201, f"Failed to set protected branch {branch_pattern} for project {project_id}: {response.status_code}, {response.text}"


# 添加用户到项目
def add_user_to_project(project_id, username):    
    data = {
        'username': username,
        'access_level': 30  # Developer
    }
    response = requests.post(f"{url}/projects/{project_id}/members", headers=headers, data=data)
    assert response.status_code == 201, f"Failed to add user {username} to project {project_id}: {response.status_code}, {response.text}"

def create_repo(group_id, username):
    # find user
    # find_user(username)

    # create project
    project_id = create_project(username, group_id)

    # set project protected branch
    # developer cannot force push or unprotect branch with lab[0-4] and bonus[1-3] prefix
    set_protected_branch(project_id, 'lab0')
    set_protected_branch(project_id, 'lab1')
    set_protected_branch(project_id, 'lab2')
    set_protected_branch(project_id, 'lab3')
    set_protected_branch(project_id, 'lab4')
    set_protected_branch(project_id, 'bonus1')
    set_protected_branch(project_id, 'bonus2')
    set_protected_branch(project_id, 'bonus3')

    # add user to project as developer
    add_user_to_project(project_id, username)

    return project_id

data_root = Path(config.data_root).resolve() / "repo"
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
    for line in tqdm(lines):
        username, name, user_id, project_id = line.strip().split(",")
        if project_id != "Failed":
            results.append(f"{username},{name},{user_id},{project_id}")
            continue
        # print(f"Creating repo for {username}")
        try:
            user = find_user(username)
            user_id = user['id']
        except Exception as e:
            results.append(f"{username},{name},Failed,Failed")
            failed += 1
            continue
        try:
            project_id = create_repo(group_id, username)
            results.append(f"{username},{name},{user_id},{project_id}")
        except Exception as e:
            results.append(f"{username},{name},{user_id},Failed")
            failed += 1
            print(e)
    with result_file.open("w") as f:
        f.write("\n".join(results))
    print(f"Saved to {result_file}")
    print(f"Total: {total}, Failed: {failed}, {failed/total:.2%}")