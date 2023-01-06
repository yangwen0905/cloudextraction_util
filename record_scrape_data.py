from account import *
import time
import xlwt
from loguru import logger
from task_start import *
import xlrd
from xlutils.copy import copy

schedule = 30
taskExcuteSuccess = 2

# 从文件中加载所有的taskid


def load_taskid():
    task_list = []
    with open("./taskids", 'r') as f:
        for line in f.readlines():
            task_list.append(line.replace("\n", "", -1))
    return task_list

# 删除文件中某个taskid
# 当任务完成时，记录任务采集的数据后会调用


def remove_taskid_from_file(taskid, filename='taskids'):
    if taskid == "":
        logger.error("taskid empty")
        return False
    taskidstr = ",".join(load_taskid())
    newtaskid = taskidstr.replace(f"{taskid}", "", 1)
    content = "\n".join(newtaskid.split(","))
    with open(filename, 'w') as f:
        f.write(content)
    return True

# 执行花费时间格式转换
# 接口返回是int类型，且单位是秒
# 这里把它转换为字符串，1h2m20s这样的格式


def get_spendtime_str(spend_sec: int):
    if spend_sec == 0:
        return "0m0s"
    if spend_sec / 60 >= 60:
        # 运行时间超过一小时
        sec_out_of_hour = spend_sec % 3600
        return f"{int(spend_sec / 3600)}h{int(sec_out_of_hour / 60)}m{sec_out_of_hour % 60}s"
    else:
        # 运行时间不足一小时
        return f"{int(spend_sec / 60)}m{spend_sec % 60}s"

# 通过taskid获取任务名称


def get_task_name(config, token, taskid):
    if type(config) != dict:
        logger.error("config type not dict")
        return ""
    headers = {
        "Authorization": token,
        "Content-Type": "application/json;charset=utf-8"
    }
    resp = requests.get(str(config['gettaskinfo']).replace(
        "{taskid}", taskid, -1), headers=headers)
    if resp.status_code != STATUSOK:
        logger.error(
            f"/api/task/gettask statuscode {resp.status_code} invalid for {taskid}")
        return ""
    resp_object = json.loads(resp.text)
    if resp_object.get('data', '') == "" or type(resp_object['data']) != dict:
        logger.error("/api/task/gettask data empty or not dict")
        return ""
    taskname = resp_object['data']['taskName']
    return taskname


# 调用progress接口获取云采集实况
def collect_task_status(config, task, token) -> list:
    if type(config) != dict:
        logger.error("api config not dict")
        return False
    if type(token) != str or token == "":
        logger.error(f"token invalid `{token}`")
        return False
    headers = {
        "Authorization": token
    }
    resp = requests.get(str(config['getprogress']).replace(
        "{taskid}", task), headers=headers)
    if resp.status_code != STATUSOK:
        logger.error(
            f"status code invalid {resp.status_code} for /api/task/progress/{task}/summary ")
        return []
    resp_object = json.loads(resp.text)
    if resp_object.get('error', '') == "" or resp_object['error'] != "success":
        logger.error(f"get {task} progress error not success")
        return []
    start_time = resp_object['data']['startTime']
    spend_time = get_spendtime_str(int(resp_object['data']['spendSec']))
    spend_time02 = resp_object['data']['spendSec']
    total_count = resp_object['data']['dataCnt']
    extract_count = resp_object['data']['extCnt']
    # if extract_count == 0:
    #     speed = 0   # rows / sec
    # else:
    #     speed = extract_count / int(spend_time)  # rows / sec
    taskname = get_task_name(config, token, task)
    if taskname == "":
        logger.error("get taskname by taskid `{task}` error")
        return []
    record = [
        task, taskname, start_time, spend_time, spend_time02, total_count, extract_count
    ]
    return record


"""
初始化excel
"""


def init_file_field(
    filename,
    fields=[
        'taskID', 'taskName',
        'startTime', 'spendTime',
        'spendTime02', 'totalCnt', 'extCnt'
    ]
):
    workbook = xlwt.Workbook(encoding='ascii')
    booksheet = workbook.add_sheet('bzy', cell_overwrite_ok=True)

    for index in range(0, len(fields)):
        booksheet.write(0, index, fields[index])
    workbook.save(filename)
    logger.info(f"initialize xlsx {filename} successfully")
    return filename


"""
写入record到xls表中
"""


def write_scrapedata(filename, record) -> bool:
    # 根据filename读取xls文件内容
    xls_data = xlrd.open_workbook(filename)
    table = xls_data.sheet_by_index(0)

    # 获取当前行数
    current_rows = table.nrows

    # copy workbook成一个xlrt对象
    # xlutils.copy.copy xlwt.WorkBook
    write_book = copy(xls_data)
    write_sheet = write_book.get_sheet(0)

    # 遍历record，把内容写入到xls文件中
    for i in range(len(record)):
        write_sheet.write(current_rows, i, record[i])
    write_book.save(filename)
    return True


def run():
    config = load_config()
    filename = init_file_field(f"scrape_status_{int(time.time())}.xls")
    token = get_token(config, get_username(), get_password())
    if token == "":
        logger.error("get token failed")
        exit(1)
    headers = {
        "Authorization": token,
        "Content-Type": "application/json;charset=utf-8"
    }
    while True:
        logger.info(
            f"checking task status {time.strftime('%Y-%m-%d %H:%M:%S',time.localtime())}")
        for task in load_taskid():
            if task == "":
                continue
            resp = requests.get(str(config['searchtasklist']).replace(
                "taskIds=", f"taskIds={task}", 1), headers=headers)
            if resp.status_code != STATUSOK:
                logger.error("status code invalid")
                message = f"get task status for task {task} occur error for `{resp.text}`\n"
                store_error_log(message)
                continue
            resp_data = json.loads(resp.text)
            if resp_data.get('data', '') != '' and resp_data['data'].get('dataList', '') != '' \
                    and len(resp_data['data']['dataList']) > 0:
                taskinfo = resp_data['data']['dataList'][0]
                if taskinfo.get('taskExecuteStatus') == taskExcuteSuccess:
                    # get scrape infomation
                    record = collect_task_status(config, task, token)
                    if type(record) != list or len(record) == 0:
                        logger.error(
                            f"get nothing from /api/progress/{task}/summary")
                        store_error_log(
                            f"nothing task scrape message for {task}", filename='scrape_error.log')
                        continue

                    # write to record file
                    # if write_scrapedata(f"scrape_status_{int(time.time())}.xls") != True:
                    if write_scrapedata(filename, record) != True:
                        logger.error("write data occur error")
                        store_error_log(
                            f"write {task} scrape data error", filename="scrape_error.log")
                        continue

                    # delete taskid in file
                    if not remove_taskid_from_file(task):
                        logger.error(
                            f"remove taskid {task} in file `taskids` error")
                        store_error_log(
                            f"rm task {task} error", filename='scrape_error.log')
                        exit(1)

        time.sleep(schedule)


run()
