from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path
import requests
import re
import tomllib
from tqdm import tqdm
from argparse import ArgumentParser
import yaml
from addict import Dict

parser = ArgumentParser()
parser.add_argument("branch", type=str, help="The branch name to get scores from")
parser.add_argument("--config", "-c", type=str, default="config.yaml", help="Path to the configuration file")
args = parser.parse_args()

with open(args.config, "r") as f:
    config = Dict(yaml.safe_load(f))

GITLAB_URL = config.gitlab.url
PRIVATE_TOKEN = config.gitlab.token

BRANCH = args.branch
DDL = config.ddl[BRANCH] # UTC+8

cpp_sha256 = config.sha256_whitelist.cpp
ocaml_sha256 = config.sha256_whitelist.ocaml

# 获取项目ID
def get_project_id(project_path):
    url = f"{GITLAB_URL}/projects/{requests.utils.quote(project_path, safe='')}"
    headers = {"PRIVATE-TOKEN": PRIVATE_TOKEN}
    
    response = requests.get(url, headers=headers)
    assert response.status_code == 200, f"Failed to get project information: {response.status_code}"
    project_info = response.json()
    return project_info['id']


# 获取仓库分支的最新 commit id
def get_latest_commit_id(project_id, branch):
    url = f"{GITLAB_URL}/projects/{project_id}/repository/branches/{branch}"
    headers = {"PRIVATE-TOKEN": PRIVATE_TOKEN}
    
    response = requests.get(url, headers=headers)
    assert response.status_code == 200, f"Failed to get branch information: {response.status_code}"
    branch_info = response.json()
    return branch_info['commit']['id']


# 获取某个提交关联的最新 Pipeline
def get_latest_pipeline(project_id, commit_id):
    url = f"{GITLAB_URL}/projects/{project_id}/pipelines"
    headers = {"PRIVATE-TOKEN": PRIVATE_TOKEN}
    params = {"sha": commit_id, "ref": BRANCH}
    
    response = requests.get(url, headers=headers, params=params)
    assert response.status_code == 200 and response.json(), f"Failed to get pipelines: {response.status_code}"
    pipeline_info = response.json()[0]  # 获取最新的pipeline
    return pipeline_info['id'], pipeline_info['status'], pipeline_info['created_at']


# 获取 Pipeline 对应的 Jobs 状态
def get_pipeline_jobs(project_id, pipeline_id):
    url = f"{GITLAB_URL}/projects/{project_id}/pipelines/{pipeline_id}/jobs"
    headers = {"PRIVATE-TOKEN": PRIVATE_TOKEN}
    
    response = requests.get(url, headers=headers)
    assert response.status_code == 200, f"Failed to get jobs information: {response.status_code}"
    return response.json()

# 获取指定 Job 的日志
def get_job_trace(project_id, job_id):
    url = f"{GITLAB_URL}/projects/{project_id}/jobs/{job_id}/trace"
    headers = {"PRIVATE-TOKEN": PRIVATE_TOKEN}

    response = requests.get(url, headers=headers)
    assert response.status_code == 200, f"Failed to get job trace: {response.status_code}"
    return response.text

def retry_pipeline_job(project_id, job_id):
    url = f"{GITLAB_URL}/projects/{project_id}/jobs/{job_id}/retry"
    headers = {"PRIVATE-TOKEN": PRIVATE_TOKEN}
    
    response = requests.post(url, headers=headers)
    assert response.status_code == 201, f"Failed to retry job: {response.status_code}"
    return response.json()

parse_error = 0
def extract_score_from_trace(trace):
    trace = trace.split("$ python3 sp25-tests/test.py $CI_COMMIT_REF_NAME .")[-1]
    # print(trace)

    assert f"Running {BRANCH} test..." in trace, "No test found"
    
    # ... Test score: 100.00 ...
    score = re.search(r"Test score: (\d+\.\d+)", trace)
    assert score, "No score found"
    
    if BRANCH == "lab0" and "Parse Error" in trace:
        global parse_error
        parse_error += 1
    
    return float(score.group(1))
        
def get_file_info(project_id, file_path, ref):
    url = f"{GITLAB_URL}/projects/{project_id}/repository/files/{requests.utils.quote(file_path, safe='')}"
    headers = {"PRIVATE-TOKEN": PRIVATE_TOKEN}
    params = {"ref": ref}
    
    response = requests.get(url, headers=headers, params=params)
    assert response.status_code == 200, f"Failed to get file information: {response.status_code}"
    return response.json()

def get_raw_file(project_id, file_path, ref):
    url = f"{GITLAB_URL}/projects/{project_id}/repository/files/{requests.utils.quote(file_path, safe='')}/raw"
    headers = {"PRIVATE-TOKEN": PRIVATE_TOKEN}
    params = {"ref": ref}

    response = requests.get(url, headers=headers, params=params)
    assert (
        response.status_code == 200
    ), f"Failed to get file information: {response.status_code}"
    
    return response.content

use_accipit = 0
use_qemu = 0
def get_score(username, name, user_id, project_id):
    commit_id = get_latest_commit_id(project_id, BRANCH)
    # print(f"Latest push commit ID: {commit_id}, Latest push time: {push_time}")
    
    # check file
    try:
        gitlab_ci_sha256 = get_file_info(project_id, ".gitlab-ci.yml", BRANCH)['content_sha256']
    except Exception as e:
        print(f"Failed to get .gitlab-ci.yml for cp-{username} {name}: {e}")
    if gitlab_ci_sha256 in ocaml_sha256['.gitlab-ci.yml']:  # OCaml template
        for file_path, hashes in ocaml_sha256.items():
            if file_path == '.gitlab-ci.yml':
                continue
            try:
                file_info = get_file_info(project_id, file_path, BRANCH)
                if file_info["content_sha256"] not in hashes:
                    print(file_info["content_sha256"])
                assert file_info, f"File {file_path} not found"
                assert file_info["content_sha256"] in hashes, f"File {file_path} has been modified"
            except Exception as e:
                print(f"Failed to check OCaml file {file_path} in cp-{username} {name}: {e}")
    else:
        for file_path, hashes in cpp_sha256.items():
            if file_path == '.gitlab-ci.yml':
                continue
            try:
                file_info = get_file_info(project_id, file_path, BRANCH)
                if file_info["content_sha256"] not in hashes:
                    print(file_info["content_sha256"])
                assert file_info, f"File {file_path} not found"
                assert file_info["content_sha256"] in hashes, f"File {file_path} has been modified"
            except Exception as e:
                print(f"Failed to check file {file_path} in cp-{username} {name}: {e}")
    
    if BRANCH == 'lab3':
        try:
            config_file = get_raw_file(project_id, 'config.toml', BRANCH)
            config = tomllib.loads(config_file.decode())
            if config.get('use_accipit', False):
                global use_accipit
                use_accipit += 1
        except Exception as e:
            print(f"Failed to check config.toml in cp-{username} {name}: {e}")
    if BRANCH in ['lab4', 'bonus1', 'bonus2']:
        try:
            config_file = get_raw_file(project_id, 'config.toml', BRANCH)
            config = tomllib.loads(config_file.decode())
            if config.get('use_qemu', False):
                global use_qemu
                use_qemu += 1
        except Exception as e:
            print(f"Failed to check config.toml in cp-{username} {name}: {e}")
            
    pipeline_id, pipeline_status, pipeline_create_at = get_latest_pipeline(project_id, commit_id)
    assert pipeline_status != 'pending', "Pipeline is still pending"
    # print(f"Pipeline ID: {pipeline_id}, Pipeline Status: {pipeline_status}")
    jobs = get_pipeline_jobs(project_id, pipeline_id)
    assert jobs, "No jobs found"
    job = jobs[0]
    trace = get_job_trace(project_id, job["id"])
    if "Timeout" in trace:
        print(f"Timeout in job {job['name']} for {username} {name}")
    # print(extract_score_from_trace(trace))
    score = extract_score_from_trace(trace)
    submit_time = datetime.strptime(pipeline_create_at, "%Y-%m-%dT%H:%M:%S.%fZ")    # UTC, 2024-02-26T14:32:00.000Z
    submit_time = submit_time + timedelta(hours=8)    # UTC+8
    # 10% punishment for each day late
    if submit_time > DDL:
        days_late = (submit_time - DDL).days + 1
        punish = max(0, 100 - days_late * 10)
        return f"{score}*{punish}%"
    return score

def process_student(username, name, user_id, project_id):
    if project_id == "Failed":
        return f"{username},{name},{user_id},{project_id},Failed"
    try:
        score = get_score(username, name, user_id, project_id)
        return f"{username},{name},{user_id},{project_id},{score}"
    except Exception as e:
        return f"{username},{name},{user_id},{project_id},Failed"
        
data_root = Path(config.data_root).resolve() / "repo"
output_root = Path(config.data_root).resolve() / "score" / BRANCH
output_root.mkdir(exist_ok=True, parents=True)

classes = sorted(data_root.glob("*.csv"))
all_total = 0
all_failed = 0
all_pass = 0
for class_file in classes:
    teacher, group_id = class_file.stem.split("-")
    print(teacher, group_id)
    result_file = output_root / f"{teacher}-{group_id}.csv"
    results = []
    with class_file.open() as f:
        lines = f.readlines()
    total = len(lines)
    failed = 0
    pass_count = 0
    with ThreadPoolExecutor(max_workers=16) as executor:
        future_to_index = {executor.submit(process_student, *line.strip().split(",")): i for i, line in enumerate(lines)}
        results_indexed = {}
        for future in tqdm(as_completed(future_to_index), total=total):
            i = future_to_index[future]
            results_indexed[i] = future.result()
        for i in range(total):
            username, name, user_id, project_id, result = results_indexed[i].split(",")
            if result == "Failed":
                failed += 1
            elif '*' not in result and float(result) == 100:
                pass_count += 1
            results.append(results_indexed[i])
    with result_file.open("w") as f:
        f.write("\n".join(results))
    print(f"Saved to {result_file}")
    print(f"Total: {total}, Failed: {failed} ({failed/total:.2%}), Pass: {pass_count} ({pass_count/total:.2%})")
    all_total += total
    all_failed += failed
    all_pass += pass_count
    
print(f"All classes:")
print(f"Total: {all_total}, Failed: {all_failed} ({all_failed/all_total:.2%}), Pass: {all_pass} ({all_pass/all_total:.2%})")
if BRANCH == 'lab0':
    print(f"Parse Error: {parse_error} ({parse_error/all_total:.2%})")
if BRANCH == 'lab3':
    print(f"Use Accipit: {use_accipit} ({use_accipit/all_total:.2%})")
if BRANCH in ['lab4', 'bonus1', 'bonus2']:
    print(f"Use QEMU: {use_qemu} ({use_qemu/all_total:.2%})")