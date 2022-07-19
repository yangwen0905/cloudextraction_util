from account import get_password, get_username
import json
import time
from loguru import logger
import requests

# 请求状态码
STATUSOK = 200


# 加载.env配置内容，返回dict对象
def load_config():
    config = {}
    with open("D:/testfile/gitgit/testdev_cloudextraction_bzy/.env", 'r') as file:
        for line in file.readlines():
            api = line.split("->")
            config[api[0].replace(
                " ", "", -1)] = api[1].replace(" ", "", -1).replace("\n", "", -1)
    return config

#  获取token


def get_token(config, username, passwd):
    postdata = {
        "username": f"{username}",
        "password": f"{passwd}",
        "grant_type": "password",
        "client_id": "octopus",
        "client_secret": "7.10.0",
        "encrypted": False
    }
    resp = requests.post(config['auth'], data=postdata)
    if resp.status_code != STATUSOK:
        return ""
    token = json.loads(resp.text).get('access_token')
    token_type = json.loads(resp.text).get('token_type')
    return f"{token_type} {token}"

# 搜索任务列表，提取所有的taskid


def list_all_task(config: dict, token: str):
    if type(config) != dict:
        return
    headers = {
        "Authorization": token,
        "Content-Type": "application/json;charset=utf-8"
    }
    resp = requests.get(config['searchtasklist'], headers=headers)
    if resp.status_code != STATUSOK:
        return []
    # 将resp.text转换成dict
    dataList = json.loads(resp.text).get("data").get('dataList')
    if len(dataList) == 0:
        return []
    taskids = []
    for data in dataList:
        if data.get('taskId', "") != "":
            taskids.append(data['taskId'])
    return taskids


#  清除所有任务的全部数据
def clean_all_data(config, taskids: list, token: str):
    if type(config) != dict:
        logger.error("api config not dict")
        return False
    if type(taskids) != list:
        logger.error("taskids not list")
        return False
    if token == "":
        logger.error("token is None")
        return False
    headers = {
        "Authorization": token,
        "Content-Type": "application/json;charset=utf-8"
    }

    for task in taskids:

        # cleandata
        resp = requests.post(str(config['cleandata']).replace(
            "{taskid}", task, -1), headers=headers)
        if resp.status_code != STATUSOK:
            logger.error("status code invalid")
            message = f"clean data for task {task} error for `{resp.text}`\n"
            store_error_log(message)
            continue
    return True


#  设置所有任务1分钟后定时启动
def schedule_all_task(config, taskids: list, token: str) -> bool:
    if type(config) != dict:
        logger.error("api config not dict")
        return False
    if type(taskids) != list:
        logger.error("taskids not list")
        return False
    if token == "":
        logger.error("token is None")
        return False
    update_schedule_data = {
        "effectiveFrom": "2020-11-06T04:38:00Z",
        "effectiveTo": "2079-06-06T00:00:00Z",
        "scheduleDate": "1",
        "scheduleTime": "1",
        "scheduleType": 4,
        "taskId": "",
        "taskStatus": 1
    }
    headers = {
        "Authorization": token,
        "Content-Type": "application/json;charset=utf-8"
    }
    for task in taskids:

        # update schedule
        update_schedule_data['taskId'] = task
        resp = requests.post(config['updateschedule'], data=json.dumps(
            update_schedule_data), headers=headers)
        if resp.status_code != STATUSOK:
            logger.error(f"{resp.text}")
            message = f"update schedule for task {task} error for `{resp.text}`"
            store_error_log(message)
            continue

        # start schedule
        resp = requests.post(str(config['startschedule']).replace(
            "{taskid}", task, -1), headers=headers)
        if resp.status_code != STATUSOK:
            logger.error(f"{resp.text}")
            message = f"start schedule for task {task} error for `{resp.text}`"
            store_error_log(message)
            continue
    logger.info("all task start schedule successfully")
    return True


# 批量停止任务定时
def stop_schedule(config, taskids, token):
    if type(config) != dict:
        logger.error("api config not dict")
        return False
    if type(taskids) != list:
        logger.error("taskids not list")
        return False
    if token == "":
        logger.error("token is None")
        return False

    headers = {
        "Authorization": token,
        "Content-Type": "application/json;charset=utf-8"
    }

    # stop schedule after schedule works
    logger.info("startting stop task schedule")
    for task in taskids:
        resp = requests.post(str(config['stopschedule']).replace(
            "{taskid}", task, -1), headers=headers)
        if resp.status_code != STATUSOK:
            logger.error("status code invalid")
            message = f"stop schedule for task {task} error for `{resp.text}`\n"
            store_error_log(message)
            continue
    logger.info("all task stop schedule successfully")
    return True


# taskid写入到文件
def store_task_to_file(filename, content: list):
    if filename == "":
        logger.error("filename empty")
        return False
    if type(content) != list or len(content) == 0:
        logger.error("taskid list length -> 0")
        return False
    with open("./taskids", 'w') as file:
        file.write("\n".join(content))
    return True


# 批量停止正在运行的任务
def stop_task(config, taskids, token):
    if type(config) != dict:
        logger.error("api config not dict")
        return False
    if type(taskids) != list:
        logger.error("taskids not list")
        return False
    if token == "":
        logger.error("token is None")
        return False
    headers = {
        "Authorization": token,
        "Content-Type": "application/json;charset=utf-8"
    }
    for task in taskids:
        resp = requests.post(str(config['stoptask']).replace(
            "{taskid}", task, -1), headers=headers)
        if resp.status_code != STATUSOK:
            logger.error("status code invalid")
            message = f"stop task for {task} error for `{resp.text}`\n"
            store_error_log(message)
            continue
    return True


# 记录日志
# 执行操作失败时会调用
def store_error_log(message, filename="error.log"):
    with open(filename, 'a') as f:
        f.write(message)


def run():
    config = load_config()
    token = get_token(config, get_username(), get_password())
    taskids = list_all_task(config, token)
    logger.info("init taskid successfully")
    if not clean_all_data(config, taskids, token):
        logger.error("clean data occur error")
        exit(1)
    logger.info("All task's data has been clean successfully")
    if not stop_task(config, taskids, token):
        logger.error("stop task occur error")
        exit(1)
    logger.info("stop all task successfully")
    if not schedule_all_task(config, taskids, token):
        logger.error("set schedule occur error")
        exit(1)
    logger.info("schedule all task")
    logger.info("waitting for schedule task")
    time.sleep(90)
    if not stop_schedule(config, taskids, token):
        logger.error("stop schedule occur error")
        exit(1)
    logger.info("stop all task schedule")
    if not store_task_to_file("./taskids", taskids):
        logger.error("save taskid to file error")
        exit(1)
    logger.info("save taskid list to file successfully")


if __name__ == '__main__':
    run()
