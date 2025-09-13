import gitlab
import yaml
import csv
import argparse


def get_or_create_subgroup(gl, parent_id, name, path, full_path):
    """获取或创建 GitLab 子组"""
    groups = gl.groups.list(search=path)
    for g in groups:
        if g.full_path == full_path:
            return g
    return gl.groups.create({'name': name, 'path': path, 'parent_id': parent_id})


def ensure_group_hierarchy(gl, course_group, term):
    """确保 course_group/term 的组织结构存在，返回 (course_group_obj, term_obj)"""
    # 获取或创建课程组
    course_group_obj = None
    groups = gl.groups.list(search=course_group)
    for g in groups:
        if g.full_path == course_group:
            course_group_obj = g
            break
    if not course_group_obj:
        course_group_obj = gl.groups.create(
            {'name': course_group, 'path': course_group})

    # 获取或创建学期子组
    term_full_path = f"{course_group}/{term}"
    term_obj = get_or_create_subgroup(
        gl, course_group_obj.id, term, term, term_full_path)

    return course_group_obj, term_obj


def get_teacher_list(data_dir, teacher=None):
    """获取教师列表"""
    import os
    if teacher:
        return [teacher]
    else:
        return [d for d in os.listdir(data_dir)
                if os.path.isdir(os.path.join(data_dir, d))]


def create_student_repo(gl, teacher_obj, sid, name, repo_prefix, upstream):
    """为单个学生创建仓库"""
    project_name = f"{repo_prefix}{sid}" if repo_prefix else sid
    project_path = f"{repo_prefix}{sid}" if repo_prefix else sid

    try:
        # 检查项目是否已存在
        existing_projects = gl.projects.list(search=project_name, owned=True)
        for p in existing_projects:
            if p.namespace['id'] == teacher_obj.id and p.path == project_path:
                print(f"仓库 {p.path_with_namespace} 已存在")
                return p

        # 创建新项目
        project = gl.projects.create({
            'name': project_name,
            'path': project_path,
            'namespace_id': teacher_obj.id,
            'visibility': 'private',
            'initialize_with_readme': True
        })
        print(f"仓库 {project.path_with_namespace} 创建成功")

        # 导入上游代码
        try:
            upstream_project = gl.projects.get(upstream)
            project.forked_from_project = upstream_project.id
            project.save()
            print(f"成功从 {upstream} 导入初始代码")
        except Exception as e:
            print(f"导入上游代码失败: {e}")

        # 添加学生为开发者
        try:
            users = gl.users.list(username=sid)
            if users:
                project.members.create({
                    'user_id': users[0].id,
                    'access_level': gitlab.DEVELOPER_ACCESS
                })
                print(f"已添加学生 {sid} 为开发者")
            else:
                print(f"警告: 用户 {sid} 在 GitLab 中不存在")
        except Exception as e:
            print(f"添加成员失败: {e}")

        # 设置分支保护
        try:
            for branch in ['main', 'lab1', 'lab2', 'lab3', 'lab4']:
                try:
                    project.protectedbranches.create({
                        'name': branch,
                        'push_access_level': gitlab.MAINTAINER_ACCESS,
                        'merge_access_level': gitlab.DEVELOPER_ACCESS
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


def init_student_repo(gl, teacher_obj, sid, name, repo_prefix, upstream):
    """为单个学生初始化仓库"""
    print(f"处理学生: {name} ({sid})")

    project = create_student_repo(
        gl, teacher_obj, sid, name, repo_prefix, upstream)
    return project is not None


def release_lab_for_student(gl, config, lab_name, teacher, sid):
    """为单个学生发布实验任务"""
    course_group = config['course']['group']
    term = config['course']['term']

    # 查找学生项目
    repo_path = f"{course_group}/{term}/{teacher}/{config['course'].get('student_repo_prefix', '')}{sid}"
    try:
        student_project = gl.projects.get(repo_path)

        # 创建实验分支
        try:
            branch = student_project.branches.get(lab_name)
            print(f"学生 {sid} 的分支 {lab_name} 已存在")
        except gitlab.exceptions.GitlabGetError:
            # 从 main 分支创建实验分支
            main_branch = student_project.branches.get('main')
            student_project.branches.create({
                'branch': lab_name,
                'ref': main_branch.commit['id']
            })
            print(f"为学生 {sid} 创建分支 {lab_name}")

        # 设置分支保护（允许学生推送但不允许强制推送）
        try:
            student_project.protectedbranches.create({
                'name': lab_name,
                'push_access_level': gitlab.DEVELOPER_ACCESS,
                'merge_access_level': gitlab.DEVELOPER_ACCESS,
                'allow_force_push': False
            })
            print(f"学生 {sid} 的分支 {lab_name} 保护设置完成")
        except Exception:
            pass  # 可能已经保护

        return True

    except Exception as e:
        print(f"处理学生 {sid} 失败: {e}")
        return False


def repo_init_for_teacher(gl, config, teacher, term_obj):
    """为单个教师初始化学生仓库"""
    import os

    course_group = config['course']['group']
    term = config['course']['term']
    upstream = config['course']['upstream']
    repo_prefix = config['course'].get('student_repo_prefix', '')
    data_dir = 'data'

    print(f"\n=== 处理教师: {teacher} ===")

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
        for row in reader:
            sid = row['id']
            name = row['name']

            if init_student_repo(gl, teacher_obj, sid, name, repo_prefix, upstream):
                student_count += 1

    print(f"\n教师 {teacher} 完成，共处理 {student_count} 个学生")
    return student_count


def lab_release_for_teacher(gl, config, lab_name, teacher):
    """为单个教师的学生发布实验任务"""
    import os

    data_dir = 'data'

    print(f"\n=== 为教师 {teacher} 的学生发布实验 ===")

    student_csv = os.path.join(data_dir, teacher, 'student.csv')
    if not os.path.exists(student_csv):
        print(f"未找到学生名单: {student_csv}")
        return 0

    student_count = 0
    with open(student_csv, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            sid = row['id']
            name = row['name']

            if release_lab_for_student(gl, config, lab_name, teacher, sid):
                student_count += 1

    print(f"教师 {teacher} 完成，共处理 {student_count} 个学生")
    return student_count


def lab_release_upstream(gl, config, lab_name):
    """处理上游仓库的实验分支创建和保护"""
    upstream = config['course']['upstream']

    print(f"处理上游仓库: {upstream}")

    # 1. 在上游仓库创建实验分支
    try:
        upstream_project = gl.projects.get(upstream)

        # 检查分支是否已存在
        try:
            branch = upstream_project.branches.get(lab_name)
            print(f"上游仓库分支 {lab_name} 已存在")
        except gitlab.exceptions.GitlabGetError:
            # 创建新分支
            main_branch = upstream_project.branches.get('main')
            upstream_project.branches.create({
                'branch': lab_name,
                'ref': main_branch.commit['id']
            })
            print(f"在上游仓库创建分支 {lab_name}")

        # 保护上游分支
        try:
            upstream_project.protectedbranches.create({
                'name': lab_name,
                'push_access_level': gitlab.NO_ACCESS,
                'merge_access_level': gitlab.MAINTAINER_ACCESS
            })
            print(f"上游分支 {lab_name} 已冻结")
        except Exception as e:
            print(f"保护上游分支失败: {e}")

    except Exception as e:
        print(f"处理上游仓库失败: {e}")
        return False

    return True


def main():
    parser = argparse.ArgumentParser(description='ZJU OS GitLab 自动化管理脚本')
    parser.add_argument('--teacher', type=str, help='指定教师名称（默认：所有教师）')

    subparsers = parser.add_subparsers(dest='subcommand', help='子命令')

    # repo-init 子命令
    repo_init_parser = subparsers.add_parser('repo-init', help='初始化学生仓库')

    # lab-release 子命令
    lab_release_parser = subparsers.add_parser('lab-release', help='发布实验任务')
    lab_release_parser.add_argument('lab', help='实验名称（如: lab1, lab2）')

    args = parser.parse_args()

    if not args.subcommand:
        parser.print_help()
        return

    # 加载配置
    with open('data/config.yaml', 'r') as f:
        config = yaml.safe_load(f)

    # 连接 GitLab
    gl = gitlab.Gitlab(url=config['gitlab']['url'],
                       private_token=config['gitlab']['private_token'])
    gl.auth()
    gl.enable_debug()

    # 执行对应的子命令
    if args.subcommand == 'repo-init':
        # 初始化学生仓库
        course_group = config['course']['group']
        term = config['course']['term']
        upstream = config['course']['upstream']
        data_dir = 'data'

        print(f"开始初始化学生仓库...")
        print(f"课程组: {course_group}, 学期: {term}, 上游仓库: {upstream}")

        # 确保组织结构存在
        course_group_obj, term_obj = ensure_group_hierarchy(
            gl, course_group, term)

        # 获取教师列表
        teachers = get_teacher_list(data_dir, args.teacher)
        print(f"处理教师: {teachers}")

        total_students = 0
        for teacher in teachers:
            student_count = repo_init_for_teacher(
                gl, config, teacher, term_obj)
            total_students += student_count

        print(f"\n仓库初始化完成！共处理 {total_students} 个学生")

    elif args.subcommand == 'lab-release':
        # 发布实验任务
        lab_name = args.lab
        data_dir = 'data'

        print(f"开始发布实验: {lab_name}")

        # 1. 处理上游仓库
        if not lab_release_upstream(gl, config, lab_name):
            return

        # 2. 获取教师列表并为每个教师的学生发布实验
        teachers = get_teacher_list(data_dir, args.teacher)
        print(f"处理教师: {teachers}")

        total_students = 0
        for teacher in teachers:
            student_count = lab_release_for_teacher(
                gl, config, lab_name, teacher)
            total_students += student_count

        print(f"\n实验 {lab_name} 发布完成！共处理 {total_students} 个学生")


if __name__ == '__main__':
    main()
