import bisect


class Cell:
    def __init__(self):
        self.count = 0
        self.stay_time = []

    def __str__(self):
        return f'{self.count} {self.stay_time}'

    def incr_counter(self):
        self.count += 1

    def add_stay_time(self, time):
        bisect.insort(self.stay_time, time)
