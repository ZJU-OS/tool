from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path
import time
import requests
import re
from tqdm import tqdm
from argparse import ArgumentParser
import yaml
from addict import Dict

parser = ArgumentParser()
parser.add_argument("branch", type=str, help="The branch name to retry jobs from")
parser.add_argument("start_time", type=str, help="The start time to filter jobs (format: YYYY-MM-DD HH:MM:SS UTC+8)")
parser.add_argument("--config", "-c", type=str, default="config.yaml", help="Path to the configuration file")
args = parser.parse_args()

with open(args.config, "r") as f:
    config = Dict(yaml.safe_load(f))

GITLAB_URL = config.gitlab.url
PRIVATE_TOKEN = config.gitlab.token

BRANCH = args.branch
START_TIME = datetime.strptime(args.start_time, "%Y-%m-%d %H:%M:%S")    # 2021-06-01 00:00:00 UTC+8

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
    return pipeline_info['id'], pipeline_info['status']


# 获取 Pipeline 对应的 Jobs 状态
def get_pipeline_jobs(project_id, pipeline_id):
    url = f"{GITLAB_URL}/projects/{project_id}/pipelines/{pipeline_id}/jobs"
    headers = {"PRIVATE-TOKEN": PRIVATE_TOKEN}
    
    response = requests.get(url, headers=headers)
    assert response.status_code == 200, f"Failed to get jobs information: {response.status_code}"
    return response.json()

def retry_pipeline_job(project_id, job_id):
    url = f"{GITLAB_URL}/projects/{project_id}/jobs/{job_id}/retry"
    headers = {"PRIVATE-TOKEN": PRIVATE_TOKEN}
    
    response = requests.post(url, headers=headers)
    assert response.status_code == 201, f"Failed to retry job: {response.status_code}"
    return response.json()

def get_job(project_id, job_id):
    url = f"{GITLAB_URL}/projects/{project_id}/jobs/{job_id}"
    headers = {"PRIVATE-TOKEN": PRIVATE_TOKEN}
    
    response = requests.get(url, headers=headers)
    assert response.status_code == 200, f"Failed to get job information: {response.status_code}"
    return response.json()



def get_job_trace(project_id, job_id):
    url = f"{GITLAB_URL}/projects/{project_id}/jobs/{job_id}/trace"
    headers = {"PRIVATE-TOKEN": PRIVATE_TOKEN}

    response = requests.get(url, headers=headers)
    assert response.status_code == 200, f"Failed to get job trace: {response.status_code}"
    return response.text

def extract_score_from_trace(trace):
    trace = trace.split("$ python3 sp25-tests/test.py $CI_COMMIT_REF_NAME .")[-1]
    # print(trace)

    assert f"Running {BRANCH} test..." in trace, "No test found"
    
    # ... Test score: 100.00 ...
    score = re.search(r"Test score: (\d+\.\d+)", trace)
    assert score, "No score found"
    
    return float(score.group(1))

def retry(username, name, user_id, project_id):
    commit_id = get_latest_commit_id(project_id, BRANCH)
    pipeline_id, pipeline_status = get_latest_pipeline(project_id, commit_id)
    jobs = get_pipeline_jobs(project_id, pipeline_id)
    assert jobs, "No jobs found"
    job = jobs[0]
    origin_trace = get_job_trace(project_id, job["id"])
    origin_score = extract_score_from_trace(origin_trace)
    if origin_score != 100:
        print(f"Score is not 100, skip retry for {username} {name}")
        return
    job_created_at = datetime.strptime(job['created_at'], "%Y-%m-%dT%H:%M:%S.%fZ")  # UTC+00:00
    if job_created_at < START_TIME - timedelta(hours=8) or job['status'] not in ['success', 'failed']:
        # retry job
        retried_job = retry_pipeline_job(project_id, job['id'])
        print(f"Retried job {retried_job['id']} for {username} {name}")
        
        # wait for job to finish
        while True:
            new_job = get_job(project_id, retried_job['id'])
            if new_job['status'] in ['success', 'failed']:
                break
            time.sleep(5)
        
        new_trace = get_job_trace(project_id, retried_job['id'])
        new_score = extract_score_from_trace(new_trace)
        
        print(f"Score changed from {origin_score} to {new_score} for {username} {name}")

def process_student(username, name, user_id, project_id):
    try:
        retry(username, name, user_id, project_id)
    except Exception as e:
        print(f"Failed to process {username} {name}: {e}")
    
data_root = Path("/Users/peipei/Documents/3-春夏/CP/sp25/scripts/repo")

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