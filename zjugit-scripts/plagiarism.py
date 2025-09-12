from concurrent.futures import ThreadPoolExecutor, as_completed
import os
from pathlib import Path
import shutil
import subprocess
import sys
import requests
import re
import zipfile
from tqdm import tqdm
import mosspy
from argparse import ArgumentParser
import yaml
from addict import Dict

parser = ArgumentParser()
parser.add_argument("branch", type=str, help="The branch name to process")
parser.add_argument("--download", "-d", action="store_true", help="Download repositories from GitLab")
parser.add_argument("--config", "-c", type=str, default="config.yaml", help="Path to the configuration file")
args = parser.parse_args()

with open(args.config, "r") as f:
    config = Dict(yaml.safe_load(f))

GITLAB_URL = config.gitlab.url
PRIVATE_TOKEN = config.gitlab.token

MOSS_USER_ID = config.moss_id
moss = mosspy.Moss(MOSS_USER_ID, "cc")

BRANCH = args.branch

teacher2name = config.teacher2name

exts = ["cpp", "hpp", "cc", "c", "h"]

data_root = Path(config.data_root).resolve() / "repo"
output_root = Path(config.data_root).resolve() / "plagiarism" / BRANCH
output_root.mkdir(exist_ok=True, parents=True)


# 获取仓库分支的最新 commit id
def get_latest_commit_id(project_id, branch):
    url = f"{GITLAB_URL}/projects/{project_id}/repository/branches/{branch}"
    headers = {"PRIVATE-TOKEN": PRIVATE_TOKEN}

    response = requests.get(url, headers=headers)
    assert (
        response.status_code == 200
    ), f"Failed to get branch information: {response.status_code}"
    branch_info = response.json()
    return branch_info["commit"]["id"]


def get_archive(project_id, commit_id, save_path: Path):
    url = f"{GITLAB_URL}/projects/{project_id}/repository/archive.zip"
    headers = {"PRIVATE-TOKEN": PRIVATE_TOKEN}
    params = {"sha": commit_id}

    response = requests.get(url, headers=headers, params=params)
    assert response.status_code == 200, f"Failed to get archive: {response.status_code}"
    save_path.parent.mkdir(exist_ok=True, parents=True)
    with open(save_path, "wb") as f:
        f.write(response.content)


def collect_and_copy_files(src_dir: Path, output_dir: Path, extensions, separator="_"):
    """收集并拷贝文件"""
    # 确保源目录存在
    if not src_dir.exists():
        print(f"错误: 源目录 {src_dir} 不存在")
        return

    # 创建输出根目录
    output_dir.mkdir(parents=True, exist_ok=True)

    # 创建对应的输出子目录
    output_dir.mkdir(parents=True, exist_ok=True)

    # 收集所有指定扩展名的文件
    for ext in extensions:
        # 递归查找所有匹配的文件
        for filepath in src_dir.rglob(f"*.{ext}"):
            # 获取相对路径
            rel_path = filepath.relative_to(src_dir)
            # 使用指定的分隔符替换路径分隔符
            new_name = str(rel_path).replace("/", separator).replace("\\", separator)
            dest_path = output_dir / new_name

            # 如果目标文件已存在，直接报错退出
            if dest_path.exists():
                print(f"错误: 目标文件 {dest_path} 已存在")
                print(f"源文件: {filepath}")
                sys.exit(1)

            # 拷贝文件
            shutil.copy2(filepath, dest_path)
            # print(f"已拷贝: {filepath} -> {dest_path}")


def process_student(teacher, username, name, user_id, project_id):
    if project_id == "Failed":
        return
    try:
        commit_id = get_latest_commit_id(project_id, BRANCH)
        archive_path = output_root / "archive" / f"{teacher}-{username}-{name}.zip"
        get_archive(project_id, commit_id, archive_path)
        # unzip to output_root / 'unzip' / f"{teacher}-{username}-{name}"
        with zipfile.ZipFile(archive_path, "r") as zip_ref:
            zip_ref.extractall(output_root / "unzip")
        # mv output_root / 'unzip' / f"cp-{username}-{commit_id}-{commit_id}"
        # to output_root / 'files' / f"{teacher}-{username}-{name}"
        src_dir = output_root / "unzip" / f"cp-{username}-{commit_id}-{commit_id}"
        dest_dir = output_root / "unzip" / f"{teacher}-{username}-{name}"
        shutil.move(src_dir, dest_dir)
        # collect_and_copy_files
        # from output_root / 'unzip' / f"{teacher}-{username}-{name}"
        # to output_root / 'files' / f"{teacher}-{username}-{name}"
        collect_and_copy_files(
            dest_dir, output_root / "files" / f"{teacher}-{username}-{name}", exts
        )
    except Exception as e:
        return
        print(f"Failed to get repo for {username}: {e}")

if args.download:
    classes = sorted(data_root.glob("*.csv"))
    for class_file in classes:
        teacher, group_id = class_file.stem.split("-")
        print(teacher, group_id)
        results = []
        with class_file.open() as f:
            lines = f.readlines()
        total = len(lines)
        with ThreadPoolExecutor() as executor:
            future_to_index = {
                executor.submit(
                    process_student, teacher2name[teacher], *line.strip().split(",")
                ): i
                for i, line in enumerate(lines)
            }
            for future in tqdm(as_completed(future_to_index), total=total):
                i = future_to_index[future]
                future.result()
    
    # collect github repos
    github_repos_root = Path(config.plagiarism.previous_path).resolve()
    for repo in github_repos_root.iterdir():
        collect_and_copy_files(
            repo, output_root / "files" / repo.name, exts
        )


def collect_source_files(directory, extensions):
    """收集指定目录下所有指定扩展名的源文件"""
    files = []
    for ext in extensions:
        files.extend(Path(directory).rglob(f"*.{ext}"))
    return [str(f) for f in files]


base_files = collect_source_files(config.plagiarism.template_path, exts)
for bf in base_files:
    moss.addBaseFile(bf)

all_files = collect_source_files(output_root / "files", exts)
files = []
for file in tqdm(all_files, desc="Processing files"):
    if not Path(file).stat().st_size > 0:
        continue
    if any(keyword in file for keyword in config.plagiarism.skip_keywords):
        continue
    if not Path(file).name.startswith("src"):
        print(file)
    
    files.append(file)
    
# files = [f for f in files if Path(f).stat().st_size > 0 and ".tab" not in f]
# files = [str(Path(f).relative_to(output_root / "files")) for f in files if Path(f).stat().st_size > 0]
for file in files:
    moss.addFile(file, display_name=str(Path(file).relative_to(output_root / "files")))

moss.setDirectoryMode(1)
moss.setIgnoreLimit(20)
bar = tqdm(total=len(files) + len(base_files), desc="Uploading files")
url = moss.send(lambda file_path, display_name: bar.update(1))
bar.close()
print()

print(f"Report Url: {url}")

wait = input("Press Enter to continue...")

moss.saveWebPage(url, output_root / "moss.html")

mosspy.download_report(url, output_root / "moss_report", connections=8, log_level=10, on_read=lambda url: print('*', end='', flush=True)) 

wait = input("Press Enter to continue...")

cmd = [
    "java", "-jar", config.plagiarism.jplag_path,
    "-l", "cpp",
    "-r", str(output_root / "jplag.zip"),
    '-bc', config.plagiarism.template_path,
    str(output_root / "unzip"),
]
subprocess.run(cmd, check=True)