import gitlab
import yaml
import csv
import argparse
import os
from datetime import datetime, timezone


class Config:
    """全局配置管理器，单例模式"""
    _instance = None
    _config = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Config, cls).__new__(cls)
        return cls._instance

    def load(self, config_path='data/config.yaml'):
        """加载配置文件"""
        if self._config is None:
            with open(config_path, 'r') as f:
                self._config = yaml.safe_load(f)
        return self._config

    @property
    def gitlab_url(self):
        return self._config['gitlab']['url']

    @property
    def gitlab_token(self):
        return self._config['gitlab']['private_token']

    @property
    def course_group(self):
        return self._config['course']['group']

    @property
    def course_term(self):
        return self._config['course']['term']

    @property
    def course_upstream(self):
        return self._config['course']['upstream']

    @property
    def student_repo_prefix(self):
        return self._config['course'].get('student_repo_prefix', '')

    @property
    def deadlines(self):
        return self._config.get('deadline', {})

    @property
    def upstream_import_url(self):
        """拼接上游仓库的完整导入URL"""
        return f"{self.gitlab_url}/{self.course_upstream}.git"

    def get_protected_branches(self):
        """获取需要保护的分支列表"""
        return ['main'] + list(self.deadlines.keys())


# 全局配置实例
config = Config()


def get_or_create_subgroup(gl, parent_id, name, path, full_path):
    """获取或创建 GitLab 子组"""
    groups = gl.groups.list(search=path)
    for g in groups:
        if g.full_path == full_path:
            return g
    return gl.groups.create({'name': name, 'path': path, 'parent_id': parent_id, 'visibility': 'public'})


def ensure_group_hierarchy(gl):
    """确保 course_group/term 的组织结构存在，返回 (course_group_obj, term_obj)"""
    course_group = config.course_group
    term = config.course_term

    # 获取或创建课程组
    course_group_obj = get_or_create_subgroup(
        gl, None, course_group, course_group, course_group)

    # 获取或创建学期子组
    term_full_path = f"{course_group}/{term}"
    term_obj = get_or_create_subgroup(
        gl, course_group_obj.id, term, term, term_full_path)

    return course_group_obj, term_obj


def get_teacher_list(data_dir, teacher=None):
    """获取教师列表"""
    if teacher:
        return [teacher]
    else:
        return [d for d in os.listdir(data_dir)
                if os.path.isdir(os.path.join(data_dir, d))]


def create_student_repo(gl, teacher_obj, sid, name):
    """为单个学生创建仓库"""
    repo_prefix = config.student_repo_prefix
    upstream_import_url = config.upstream_import_url

    project_name = f"{repo_prefix}{sid}" if repo_prefix else sid

    try:
        # 检查项目是否已存在
        existing_projects = gl.projects.list(search=project_name, owned=True)
        project = None
        for p in existing_projects:
            if p.namespace['id'] == teacher_obj.id and p.name == project_name:
                project = p
                break

        if project is None:
            # 创建新项目
            print(upstream_import_url)
            project = gl.projects.create({
                'name': project_name,
                'namespace_id': teacher_obj.id,
                'visibility': 'private',
                'import_url': upstream_import_url
            })
            print(f"仓库 {project.path_with_namespace} 创建成功")

        # 添加学生为开发者
        try:
            users = gl.users.list(username=sid)
            # 此时用户一定存在，因为在 init_student_repo 中已经检查过了
            project.members.create({
                'user_id': users[0].id,
                'access_level': gitlab.const.AccessLevel.DEVELOPER
            })
            print(f"已添加学生 {sid} 为开发者")
        except Exception as e:
            print(f"添加成员失败: {e}")

        # 设置分支保护
        try:
            protected_branches = config.get_protected_branches()
            for branch in protected_branches:
                try:
                    project.protectedbranches.create({
                        'name': branch,
                        'push_access_level': gitlab.const.AccessLevel.DEVELOPER,
                        'merge_access_level': gitlab.const.AccessLevel.DEVELOPER,
                    })
                    print(f"已保护分支 {branch}")
                except Exception:
                    pass  # 分支可能不存在或已保护
        except Exception as e:
            print(f"设置分支保护失败: {e}")

        return project

    except Exception as e:
        print(f"创建仓库失败: {e}")
        return None


def init_student_repo(gl, teacher_obj, sid, name):
    """为单个学生初始化仓库"""
    print(f"处理学生: {name} ({sid})")

    # 首先检查用户在GitLab中是否存在
    try:
        users = gl.users.list(username=sid)
        if not users:
            print(f"✗ 用户 {sid}（{name}）在 GitLab 中不存在，跳过创建仓库")
            return False
        print(f"✓ 用户 {sid}（{name}）在 GitLab 中存在")
    except Exception as e:
        print(f"查询用户 {sid}（{name}）时出错: {e}，跳过创建仓库")
        return False

    project = create_student_repo(gl, teacher_obj, sid, name)
    return project is not None


def repo_init_for_teacher(gl, teacher, term_obj):
    """为单个教师初始化学生仓库"""
    course_group = config.course_group
    term = config.course_term
    data_dir = 'data'

    # 获取或创建教师子组
    teacher_full_path = f"{course_group}/{term}/{teacher}"
    teacher_obj = get_or_create_subgroup(
        gl, term_obj.id, teacher, teacher, teacher_full_path)

    # 读取学生名单
    student_csv = os.path.join(data_dir, teacher, 'student.csv')
    if not os.path.exists(student_csv):
        print(f"未找到学生名单文件: {student_csv}，跳过")
        return 0

    student_count = 0
    with open(student_csv, newline='') as csvfile:
        reader = csv.DictReader(csvfile)

        # 检查CSV文件是否有必要的列
        if reader.fieldnames is None:
            print(f"错误: CSV文件 {student_csv} 为空或没有标题行")
            return 0

        # 清理字段名，去除可能的BOM和空白字符
        fieldnames = [field.strip() for field in reader.fieldnames]

        if 'id' not in fieldnames or 'name' not in fieldnames:
            print(f"错误: CSV文件 {student_csv} 缺少必要的列 'id' 或 'name'")
            print(f"当前列名: {reader.fieldnames}")
            print(f"清理后列名: {fieldnames}")
            return 0

        for row in reader:
            # 处理可能带BOM的键名
            id_key = next((k for k in row.keys() if k.strip() == 'id'), None)
            name_key = next(
                (k for k in row.keys() if k.strip() == 'name'), None)

            if not id_key or not name_key or not row.get(id_key) or not row.get(name_key):
                print(f"警告: 跳过空行或缺少id/name的行: {row}")
                continue

            sid = row[id_key].strip()
            name = row[name_key].strip()

            if init_student_repo(gl, teacher_obj, sid, name):
                student_count += 1

    print(f"共处理 {student_count} 个学生")
    return student_count


def repo_delete_for_teacher(gl, teacher):
    """为单个教师删除所有学生仓库"""
    course_group = config.course_group
    term = config.course_term
    repo_prefix = config.student_repo_prefix
    data_dir = 'data'

    student_csv = os.path.join(data_dir, teacher, 'student.csv')
    if not os.path.exists(student_csv):
        print(f"未找到学生名单文件: {student_csv}，跳过")
        return 0

    deleted_count = 0

    with open(student_csv, newline='', encoding='utf-8-sig') as csvfile:
        reader = csv.DictReader(csvfile)

        # 检查CSV文件是否有必要的列
        if reader.fieldnames is None:
            print(f"错误: CSV文件 {student_csv} 为空或没有标题行")
            return 0

        # 清理字段名，去除可能的BOM和空白字符
        fieldnames = [field.strip() for field in reader.fieldnames]

        if 'id' not in fieldnames or 'name' not in fieldnames:
            print(f"错误: CSV文件 {student_csv} 缺少必要的列 'id' 或 'name'")
            print(f"当前列名: {reader.fieldnames}")
            print(f"清理后列名: {fieldnames}")
            return 0

        for row in reader:
            # 处理可能带BOM的键名
            id_key = next((k for k in row.keys() if k.strip() == 'id'), None)
            name_key = next(
                (k for k in row.keys() if k.strip() == 'name'), None)

            if not id_key or not name_key or not row.get(id_key) or not row.get(name_key):
                print(f"警告: 跳过空行或缺少id/name的行: {row}")
                continue

            sid = row[id_key].strip()
            name = row[name_key].strip()

            # 构建仓库路径
            repo_path = f"{course_group}/{term}/{teacher}/{repo_prefix}{sid}"

            try:
                # 查找并删除学生项目
                student_project = gl.projects.get(repo_path)
                student_project.delete()
                print(f"✓ 已删除仓库: {repo_path}")
                deleted_count += 1
            except gitlab.exceptions.GitlabGetError:
                print(f"✗ 仓库不存在: {repo_path}")
            except Exception as e:
                print(f"✗ 删除仓库 {repo_path} 失败: {e}")

    print(f"共删除 {deleted_count} 个仓库")
    return deleted_count


def student_check_for_teacher(gl, teacher):
    """为单个教师检查学生在GitLab中的存在状态"""
    data_dir = 'data'

    student_csv = os.path.join(data_dir, teacher, 'student.csv')
    if not os.path.exists(student_csv):
        print(f"未找到学生名单文件: {student_csv}，跳过")
        return []

    missing_students = []
    found_students = []

    with open(student_csv, newline='', encoding='utf-8-sig') as csvfile:
        reader = csv.DictReader(csvfile)

        # 检查CSV文件是否有必要的列
        if reader.fieldnames is None:
            print(f"错误: CSV文件 {student_csv} 为空或没有标题行")
            return []

        # 清理字段名，去除可能的BOM和空白字符
        fieldnames = [field.strip() for field in reader.fieldnames]

        if 'id' not in fieldnames or 'name' not in fieldnames:
            print(f"错误: CSV文件 {student_csv} 缺少必要的列 'id' 或 'name'")
            print(f"当前列名: {reader.fieldnames}")
            print(f"清理后列名: {fieldnames}")
            return []

        for row in reader:
            # 处理可能带BOM的键名
            id_key = next((k for k in row.keys() if k.strip() == 'id'), None)
            name_key = next(
                (k for k in row.keys() if k.strip() == 'name'), None)

            if not id_key or not name_key or not row.get(id_key) or not row.get(name_key):
                print(f"警告: 跳过空行或缺少id/name的行: {row}")
                continue

            sid = row[id_key].strip()
            name = row[name_key].strip()

            # 在GitLab中查询用户
            try:
                users = gl.users.list(username=sid)
                if users:
                    found_students.append({'sid': sid, 'name': name})
                    print(f"✓ 找到用户: {sid}（{name}）")
                else:
                    missing_students.append({'sid': sid, 'name': name})
                    print(f"✗ 未找到用户: {sid}（{name}）")
            except Exception as e:
                print(f"查询用户 {sid}（{name}）时出错: {e}")
                missing_students.append({'sid': sid, 'name': name})

    print(f"找到的学生: {len(found_students)} 人，未找到的学生: {len(missing_students)} 人")

    if missing_students:
        print("未找到的学生列表:")
        for student in missing_students:
            print(f"  - {student['sid']}（{student['name']}）")

    return missing_students


def get_expired_labs():
    """获取所有已过期的实验列表"""
    deadlines = config.deadlines
    expired_labs = []
    current_time = datetime.now(timezone.utc)

    print(f"当前时间: {current_time.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    print("检查各实验 DDL 状态:")

    for lab_name, deadline_str in deadlines.items():
        try:
            # 解析时间字符串，格式："2025-10-01 23:59:59 +0800"
            deadline = datetime.strptime(deadline_str, "%Y-%m-%d %H:%M:%S %z")

            if current_time > deadline:
                expired_labs.append(lab_name)
                print(f"  {lab_name}: {deadline_str} - 已过期")
            else:
                print(f"  {lab_name}: {deadline_str} - 未过期")
        except ValueError as e:
            print(f"  {lab_name}: 时间格式错误 '{deadline_str}' - {e}")

    return expired_labs


def is_lab_deadline_passed(lab_name):
    """检查实验 DDL 是否已超过"""
    deadlines = config.deadlines
    if lab_name not in deadlines:
        print(f"警告: 实验 {lab_name} 的 DDL 未在配置中找到")
        return False

    deadline_str = deadlines[lab_name]
    try:
        # 解析时间字符串，格式："2025-10-01 23:59:59 +0800"
        deadline = datetime.strptime(deadline_str, "%Y-%m-%d %H:%M:%S %z")
        current_time = datetime.now(timezone.utc)

        is_passed = current_time > deadline
        print(f"实验 {lab_name} DDL: {deadline_str}, 当前时间: {current_time.strftime('%Y-%m-%d %H:%M:%S %Z')}, 是否超过: {is_passed}")
        return is_passed
    except ValueError as e:
        print(f"错误: 无法解析 DDL 时间格式 '{deadline_str}': {e}")
        return False


def close_lab_for_student(gl, lab_name, teacher, sid):
    """为单个学生关闭实验分支的推送权限"""
    course_group = config.course_group
    term = config.course_term
    repo_prefix = config.student_repo_prefix

    # 查找学生项目
    repo_path = f"{course_group}/{term}/{teacher}/{repo_prefix}{sid}"
    try:
        student_project = gl.projects.get(repo_path)

        # 检查实验分支是否存在
        try:
            branch = student_project.branches.get(lab_name)
        except gitlab.exceptions.GitlabGetError:
            print(f"学生 {sid} 的分支 {lab_name} 不存在，跳过")
            return False

        # 更新分支保护设置，禁止推送
        try:
            # 先删除现有保护
            try:
                protected_branch = student_project.protectedbranches.get(
                    lab_name)
                protected_branch.delete()
            except gitlab.exceptions.GitlabGetError:
                pass  # 分支可能没有保护

            # 重新创建保护，设置为 NO_ACCESS
            student_project.protectedbranches.create({
                'name': lab_name,
                'push_access_level': gitlab.const.AccessLevel.NO_ACCESS,
                'merge_access_level': gitlab.const.AccessLevel.NO_ACCESS,
                'allow_force_push': False
            })
            print(f"学生 {sid} 的分支 {lab_name} 推送权限已关闭")
            return True

        except Exception as e:
            print(f"关闭学生 {sid} 分支 {lab_name} 推送权限失败: {e}")
            return False

    except Exception as e:
        print(f"处理学生 {sid} 失败: {e}")
        return False


def lab_close_for_teacher(gl, teacher, lab_name):
    """为单个教师的学生关闭实验分支推送权限"""
    data_dir = 'data'

    student_csv = os.path.join(data_dir, teacher, 'student.csv')
    if not os.path.exists(student_csv):
        print(f"未找到学生名单: {student_csv}")
        return 0

    student_count = 0
    with open(student_csv, newline='', encoding='utf-8-sig') as csvfile:
        reader = csv.DictReader(csvfile)

        # 检查CSV文件是否有必要的列
        if reader.fieldnames is None:
            print(f"错误: CSV文件 {student_csv} 为空或没有标题行")
            return 0

        # 清理字段名，去除可能的BOM和空白字符
        fieldnames = [field.strip() for field in reader.fieldnames]

        if 'id' not in fieldnames or 'name' not in fieldnames:
            print(f"错误: CSV文件 {student_csv} 缺少必要的列 'id' 或 'name'")
            print(f"当前列名: {reader.fieldnames}")
            print(f"清理后列名: {fieldnames}")
            return 0

        for row in reader:
            # 处理可能带BOM的键名
            id_key = next((k for k in row.keys() if k.strip() == 'id'), None)
            name_key = next(
                (k for k in row.keys() if k.strip() == 'name'), None)

            if not id_key or not name_key or not row.get(id_key) or not row.get(name_key):
                print(f"警告: 跳过空行或缺少id/name的行: {row}")
                continue

            sid = row[id_key].strip()
            name = row[name_key].strip()

            if close_lab_for_student(gl, lab_name, teacher, sid):
                student_count += 1

    print(f"共处理 {student_count} 个学生")
    return student_count


def execute_for_teachers(gl, teacher_filter, operation_func, *args, **kwargs):
    """
    为指定的教师执行操作的通用执行器

    Args:
        gl: GitLab 连接实例
        teacher_filter: 教师过滤器，None 表示所有教师，字符串表示特定教师
        operation_func: 要为每个教师执行的操作函数
        *args: 传递给操作函数的额外参数
        **kwargs: 传递给操作函数的额外关键字参数

    Returns:
        tuple: (总计数, 所有结果列表)
    """
    # 获取教师列表
    teachers = get_teacher_list('data', teacher_filter)
    print(f"处理教师: {teachers}")

    total_count = 0
    all_results = []

    for teacher in teachers:
        print(f"\n--- 处理教师: {teacher} ---")
        result = operation_func(gl, teacher, *args, **kwargs)

        # 根据结果类型处理计数
        if isinstance(result, int):
            total_count += result
        elif isinstance(result, list):
            all_results.extend(result)
            total_count += len(result)
        elif isinstance(result, dict) and 'count' in result:
            total_count += result['count']
            if 'data' in result:
                all_results.extend(result['data'])

    return total_count, all_results


def main():
    parser = argparse.ArgumentParser(description='ZJU OS GitLab 自动化管理脚本')
    parser.add_argument('-t', '--teacher', type=str, help='指定教师名称（默认：所有教师）')
    parser.add_argument('-v', '--verbose',
                        action='store_true', help='启用详细输出和调试信息')

    subparsers = parser.add_subparsers(dest='subcommand', help='子命令')

    # student-check 子命令
    student_check_parser = subparsers.add_parser(
        'student-check', help='查询学生信息')

    # repo-init 子命令
    repo_init_parser = subparsers.add_parser('repo-init', help='初始化学生仓库')

    # repo-delete 子命令
    repo_delete_parser = subparsers.add_parser('repo-delete', help='删除学生仓库')

    # lab-close 子命令
    lab_close_parser = subparsers.add_parser('lab-close', help='关闭实验提交')
    lab_close_parser.add_argument(
        'lab', nargs='?', help='实验名称（如: lab1, lab2）。如果不提供，将自动关闭所有已过期的实验')

    args = parser.parse_args()

    if not args.subcommand:
        parser.print_help()
        return

    # 加载全局配置
    config.load()

    # 连接 GitLab
    gl = gitlab.Gitlab(url=config.gitlab_url,
                       private_token=config.gitlab_token)
    gl.auth()
    if args.verbose:
        gl.enable_debug()

    # 执行对应的子命令
    if args.subcommand == 'student-check':
        # 查询学生信息
        print(f"开始查询学生信息...")

        total_missing, all_missing_students = execute_for_teachers(
            gl, args.teacher, student_check_for_teacher)

        print(f"\n=== 查询完成 ===")
        print(f"总共无法查询到的学生: {total_missing} 人")

        if all_missing_students:
            print("\n所有无法查询到的学生列表:")
            for student in all_missing_students:
                print(f"  - {student['sid']}（{student['name']}）")
        else:
            print("所有学生都能在 GitLab 中找到！")

    elif args.subcommand == 'repo-init':
        # 初始化学生仓库
        print(f"开始初始化学生仓库...")
        print(
            f"课程组: {config.course_group}, 学期: {config.course_term}, 上游仓库: {config.course_upstream}")

        # 确保组织结构存在
        course_group_obj, term_obj = ensure_group_hierarchy(gl)

        total_students, _ = execute_for_teachers(
            gl, args.teacher, repo_init_for_teacher, term_obj)

        print(f"\n仓库初始化完成！共处理 {total_students} 个学生")

    elif args.subcommand == 'repo-delete':
        # 删除学生仓库
        print(f"开始删除学生仓库...")
        print("警告: 这将永久删除所有学生仓库，请确认此操作！")

        total_deleted, _ = execute_for_teachers(
            gl, args.teacher, repo_delete_for_teacher)

        print(f"\n仓库删除完成！共删除 {total_deleted} 个仓库")

    elif args.subcommand == 'lab-close':
        # 关闭实验提交
        lab_name = args.lab

        if lab_name:
            # 如果指定了实验名称，处理单个实验
            print(f"开始关闭实验: {lab_name}")

            # 检查 DDL 是否已过
            if not is_lab_deadline_passed(lab_name):
                print(f"实验 {lab_name} 的 DDL 尚未超过，无法关闭")
                return

            total_students, _ = execute_for_teachers(
                gl, args.teacher, lab_close_for_teacher, lab_name)

            print(f"\n实验 {lab_name} 关闭完成！共处理 {total_students} 个学生")
        else:
            # 如果没有指定实验名称，自动检测并关闭所有过期的实验
            print("开始自动检测并关闭过期实验...")

            expired_labs = get_expired_labs()

            if not expired_labs:
                print("没有发现过期的实验")
                return

            print(f"发现 {len(expired_labs)} 个过期实验: {', '.join(expired_labs)}")

            total_students = 0
            for expired_lab in expired_labs:
                print(f"\n--- 处理过期实验: {expired_lab} ---")
                lab_total, _ = execute_for_teachers(
                    gl, args.teacher, lab_close_for_teacher, expired_lab)
                total_students += lab_total

            print(f"\n所有过期实验关闭完成！共处理 {total_students} 个学生操作")


if __name__ == '__main__':
    main()
