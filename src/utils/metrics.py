import time

class Metrics:
    def __init__(self):
        self.start_time = time.time()
        self.requests = 0

    def record_request(self):
        self.requests += 1

    def uptime(self):
        return time.time() - self.start_time
