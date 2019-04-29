# coding=utf-8
import os
import sys
import invoke
import pymysql
import time
import datetime
from croniter import croniter
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor
from apscheduler.schedulers.background import BackgroundScheduler
from multiprocessing import Pipe, Process, cpu_count
from apscheduler.triggers.cron import CronTrigger


'''
+---------+-----------+------+-----+-------------------+-----------------------------+
| Field   | Type      | Null | Key | Default           | Extra                       |
+---------+-----------+------+-----+-------------------+-----------------------------+
| id      | int(11)   | NO   | PRI | NULL              | auto_increment              |
| minute  | char(20)  | YES  |     | *                 |                             |
| hour    | char(20)  | YES  |     | *                 |                             |
| day     | char(20)  | YES  |     | *                 |                             |
| month   | char(20)  | YES  |     | *                 |                             |
| week    | char(20)  | YES  |     | *                 |                             |
| func    | text      | YES  |     | NULL              |                             |
| created | datetime  | YES  |     | CURRENT_TIMESTAMP |                             |
| changed | timestamp | NO   |     | CURRENT_TIMESTAMP | on update CURRENT_TIMESTAMP |
+---------+-----------+------+-----+-------------------+-----------------------------+

'''

db_parm = {  # 数据库配置
    'host': 'localhost',
    'port': 3306,
    'user': 'xxx',
    'password': 'xxx',
    'charset': 'utf8',
    'database': 'xxx'
}

cron_minute = 0.1  # 间隔时间设置

executors_single = {  # 单核线程池
    'default': ProcessPoolExecutor(1),
}

executors = {
    # 等cpu数目的线程池
    'default': ProcessPoolExecutor(cpu_count()),
}

conn = pymysql.connect(**db_parm)
cursor = conn.cursor()

son, father = Pipe()


def execute(command, shell=True):
    result = invoke.run(command)
    print(time.ctime(), f'f({command})', result.return_code)


class Sched:
    def __init__(self, table='cron1'):
        print(time.ctime(), 'Sched init')
        self.sched2 = BackgroundScheduler(executors=executors)
        self.table = table
        self.sched2.start()
        self.now = self.in_time = self.future = datetime.datetime.now()

    def exit(self):

        if self.sched2.running:
            self.sched2.shutdown(wait=True)
        os._exit(0)

    def cron_parse(self, l):
        """
        解析一个crontab表达式，返回下一次的执行时间
        https://pypi.org/project/croniter/
        """
        cron = f'{l[0]} {l[1]} {l[2]} {l[3]} {l[4]}'
        iter = croniter(cron, self.now)

        self.in_time = iter.get_next(datetime.datetime)

    def add_job(self, now, future):
        self.now = now
        self.future = future

        sql = f'select id, minute, hour, day, month, week, func from {self.table};'
        cursor.execute(sql)
        fetch = cursor.fetchall()

        for record in fetch:
            self.cron_parse(record[1:6])  # 解析

            if self.now < self.in_time < self.future:
                print('该条记录将要执行', record)

                self.sched2.add_job(execute, 'date',  # 按日期执行
                                    run_date=self.in_time,  # 运行时间
                                    id=str(record[0]),
                                    args=(record[-1],),  # 要执行的命令
                                    replace_existing=True
                                    )


def timer():
    """异步定时器"""
    while 1:
        n = datetime.datetime.now()
        son.send((n.year, n.month, n.day, n.hour, n.minute, n.second))
        time.sleep(60 * cron_minute)


def start_timer():
    # 启动计时器
    timer_process = Process(target=timer)
    timer_process.daemon = True
    timer_process.start()


def main():
    start_timer()  # 启动计时器
    sched = Sched()  # 启动定时任务
    try:

        while True:
            # 接收时间刻度
            timw_tuple = father.recv()  # 主进程收到计时
            print(time.ctime(), '收到了计时，开始查库检测')
            timw = datetime.datetime(*timw_tuple)  # 收到的时间
            period = timw + datetime.timedelta(minutes=cron_minute)  # 一段时间之后的时间

            # 查库、解析、修改任务
            sched.add_job(timw, period)

    except (KeyboardInterrupt, SystemExit):
        # 阻塞等待任务结束
        sched.exit()


if __name__ == '__main__':
    main()
