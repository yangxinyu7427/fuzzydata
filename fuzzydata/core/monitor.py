import time
import psutil
import os
import threading


class Monitor:

    def __init__(self, pid):
        self.p = psutil.Process(pid)
        self.count = 0
        self.memSum = 0
        # self.cpuSum = 0
        self.interval = 0
        self.sign = False

    def begin(self):
        self.interval = 0.00000001  # 获取内存使用情况轮询时间间隔
        while True:
            if self.sign:
                break
            # cpu_percent = self.p.cpu_percent(interval=0.5)
            mem_percent = self.p.memory_percent()
            self.memSum += mem_percent
            # self.cpuSum += cpu_percent
            self.count += 1
            time.sleep(self.interval)

    def end(self) -> float:
        self.sign = True
        time.sleep(self.interval)
        if self.count > 0:
            return self.memSum / self.count
        else:
            return 0
