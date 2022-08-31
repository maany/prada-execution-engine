from multiprocessing import Pool, Process
import logging
import multiprocessing
from typing import List
from core import PipelineElement

logger = logging.getLogger(__name__)

context = multiprocessing.get_context('spawn')


class Worker(multiprocessing.Process):
    def __init__(self, task_queue: multiprocessing.Queue, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.task_queue = task_queue

    @classmethod
    def register(cls, context: multiprocessing.context.BaseContext):
        """_summary_ Register this worker class with the multiprocessing context

        :param context: _description_ the multiprocessing context to register the worker to
        :type context: multiprocessing.context.BaseContext
        """
        context.process = cls

    def work(self, pipeline_element: PipelineElement):
        logger.debug("{worker}: Started \n".format(worker=self.name))
        while True:
            pipeline_element = self.task_queue.get()
            pipeline_element.report["executor_process"] = self.name
            try:
                pipeline_element.execute()
            except Exception as e:
                # An exception happened in this thread
                err_msg = "Error during parallel execution of {element}".format(
                    element=pipeline_element.name)
                logger.error("{worker}: {err_msg}".format(
                    worker=self.name, err_msg=err_msg))
                logger.debug("{worker}: {error} occurred when executing {element}".format(worker=self.name,
                                                                                          error=type(
                                                                                              e),
                                                                                          element=pipeline_element.name,
                                                                                          ), exc_info=True)
            finally:
                # Mark this task as done, whether an exception happened or not
                logger.debug("{worker}: Dequeue {element}. Test exit code was: {code}".format(
                    worker=self.name,
                    element=pipeline_element.name,
                    code=pipeline_element.exit_code))
                self.task_queue.task_done()


class Pool(multiprocessing.Pool):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.context = multiprocessing.get_context('spawn')
        self.queue = self.context.Queue()
        self.workers: List[Worker] = []

    def create_worker(self):
        self.workers.append(Worker(self.queue))

    def add_task(self, task):
        self.queue.put(task)

    def wait_completion(self):
        self.queue.join()
