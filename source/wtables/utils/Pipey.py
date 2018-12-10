#!/usr/bin/env python3
#https://gist.github.com/smhanov/8fb48199338045fc5e69fd615211c84c
from multiprocessing import Process, Queue
import sys
import logging
import traceback
import inspect

# TODO: Make these more unique
STOP = "STOP"
SHUTDOWN = "SHUTDOWN"
SHUTDOWN_LAST = "SHUTDOWN_LAST"

log = None


class Task:
    def __init__(self, id, fn, inputQueue, outputQueue, multiplicity):
        self.id = id
        self.fn = fn
        self.inputQueue = inputQueue
        self.outputQueue = outputQueue
        self.multiplicity = multiplicity

    def start(self):
        self.process = Process(target=self.main, args=(self.inputQueue, self.outputQueue))
        self.process.start()

    def main(self, inputQueue, outputQueue):
        self.inputQueue = inputQueue
        self.outputQueue = outputQueue

        if inspect.isfunction(self.fn):
            logger = logging.getLogger(str(self.id) + ":" +
                                       self.fn.__name__)
        else:
            logger = logging.getLogger(str(self.id) + ":" +
                                       type(self.fn).__name__)
        global log
        log = lambda a: logger.debug(a)

        try:
            if hasattr(self.fn, "init"):
                self.fn.init()

            log("Running")

            while True:
                input = self.inputQueue.get()
                log("Input is {}".format(input))
                if input == SHUTDOWN: break
                if input == SHUTDOWN_LAST:
                    self.outputQueue.put(STOP)
                    break
                if input == STOP:
                    for i in range(self.multiplicity - 1):
                        self.inputQueue.put(SHUTDOWN)
                    self.inputQueue.put(SHUTDOWN_LAST)
                    continue

                result = self.fn(input)
                if inspect.isgenerator(result):
                    for x in result:
                        if x == STOP:
                            self.inputQueue.put(STOP)
                            break
                        self.outputQueue.put(x)
                else:
                    if result == STOP:
                        self.inputQueue.put(STOP)
                    else:
                        self.outputQueue.put(result)

            log("Shutting down")
            if hasattr(self.fn, "shutdown"):
                self.fn.shutdown()

        except KeyboardInterrupt:
            pass
        except Exception:
            print("For {}".format(self.fn))
            raise


class Pipeline:
    def __init__(self):
        self.tasks = []
        self.inputQueue = Queue(1)
        self.outputQueue = Queue(1)
        self.nextId = 1

    def run(self, arg=None):

        for task in self.tasks:
            task.start()

        self.inputQueue.put(arg)
        while True:
            x = self.outputQueue.get()
            if x == STOP: break

    def add(self, fn, fanOut=1):
        inputQueue = self.inputQueue
        outputQueue = self.outputQueue
        if len(self.tasks):
            inputQueue = Queue(2)
            for task in self.tasks:
                if task.outputQueue == self.outputQueue:
                    task.outputQueue = inputQueue

        for i in range(fanOut):
            task = Task(self.nextId, fn, inputQueue, outputQueue, fanOut)
            self.nextId += 1
            self.tasks.append(task)