import logging
import json
import traceback
from abc import ABCMeta, abstractmethod
from collections import OrderedDict
from core.exceptions import PreConditionNotSatisfiedError

class Pool:
    """

    """
    tasks: list["PipelineElement"] = []
    stages: list["Stage"] = []
    logger = logging.getLogger(__name__)

    @staticmethod
    def register_task(task: "PipelineElement") -> None:
        """
        Register a test with the pool.
        """
        Pool.tasks.append(task)

    @staticmethod
    def register_stage(stage: "Stage") -> None:
        """
        Register a stage with the pool.
        """
        Pool.stages.append(Stage)
        Pool.logger.debug(f"Registered {stage.name} in task pool")

    @staticmethod
    def get_all_tasks():
        return Pool.tasks

    @staticmethod
    def get_all_stages():
        return Pool.stages


class PipelineElement:
    """
    An executable and composable entity that represents the test pipeline and provides
    functions to execute and log events from the test pipeline.
    """
    __metaclass__ = ABCMeta

    def __init__(self, name, executable_type):
        self.name = name
        self.type = executable_type
        self.pipeline_elements = []
        self.exit_code = 0
        self.report = OrderedDict(
            {'name': self.name, 'result': '', 'type': self.type})
        self.logger = logging.getLogger(__name__)
        self.hard_error_pre_condition = True

    def append_to_pipeline(self, pipeline_element):
        self.pipeline_elements.append(pipeline_element)

    def extend_pipeline(self, pipeline_elements):
        for pipeline_element in pipeline_elements:
            self.append_to_pipeline(pipeline_element)

    def pre_condition(self):
        """
        Override if certain checks need to be performed before executing the pipeline.
        Copy artifacts etc.
        """
        pass

    def pre_condition_handler(self):
        return_status = False
        try:
            self.pre_condition()
            return_status = True
        except PreConditionNotSatisfiedError as err:
            if self.hard_error_pre_condition:
                self.logger.error("The pre condition check for {type} {name} was not satisfied. "
                                  "Therefore, the execution of the following pipeline elements is "
                                  "being skipped: {elements}".format(
                                      name=self.name,
                                      type=self.type,
                                      elements=', '.join(
                                          ["{element}".format(element=element)
                                           for element in self.pipeline_elements]
                                      )))
                self.report["result"] = "exec_fail"
                self.exit_code = 1
            else:
                self.logger.warning("The pre condition check for {type} {name} was not satisfied. "
                                    "However, we are continuing the execution of the tests".
                                    format(name=self.name, type=self.type))
                self.exit_code = 4  # pre_condition failed
                return_status = True
            self.logger.info("Exception info: {error}".format(
                error=err.message), exc_info=True)
            self.report["error"] = err.message
            self.report["trace"] = traceback.format_exc()
            self.report["exit_code"] = self.exit_code

        return return_status

    def run(self):
        for element in self.pipeline_elements:
            element.execute()

    def execute(self):
        if not self.pre_condition_handler():
            self.logger.error("Pre condition failed for {type}: {name}".format(
                name=self.name, type=self.type))
            return
        self.logger.info("Executing Pipeline for {type} {name}".format(
            name=self.name, type=self.type))
        pipeline_elements_csv = ', '.join(
            [element.name for element in self.pipeline_elements])
        self.logger.info("{type} {name} has the following pipeline elements registered: {pipeline_elements}".format(
            name=self.name,
            type=self.type,
            pipeline_elements=pipeline_elements_csv))
        self.run()
        # Update report
        self.post_process()

    def post_process(self):
        """ Generate Report and update Exit Code """
        pipeline_reports = [
            element.report for element in self.pipeline_elements]
        self.report['total_elements'] = len(pipeline_reports)
        self.report['element_reports'] = pipeline_reports
        exit_codes = set([test.exit_code for test in self.pipeline_elements])
        if self.exit_code == 4:
            self.report["result"] = "pre condition failed! "
        if 1 in exit_codes:
            self.exit_code = 1
            # switch to codes someday
            self.report["result"] += "some or all tests fail"
        elif 3 in exit_codes:
            self.exit_code = 3
            self.report["result"] += "warnings present"
        else:
            self.report["result"] += "all tests passed"


class Stage(PipelineElement):
    """
    A special pipeline element that groups other pipeline elements into a stage.
    """

    __metaclass__ = ABCMeta

    def __init__(self, name):
        PipelineElement.__init__(self, name, "Stage")
        self.name = name
        self.hard_error_pre_condition = True

    @abstractmethod
    def create_pipeline(self):
        pass

    def pre_condition_handler(self):
        return_status = super(Stage, self).pre_condition_handler()
        if not return_status:
            self.logger.api(json.dumps(self.report, indent=4))
        return return_status

    def post_process(self):
        """ Generate report and update exit code"""
        super(Stage, self).post_process()
        self.logger.api(json.dumps(self.report, indent=4))


class StageType(ABCMeta):
    """
    Automatically register a class that has __metaclass__ = StageType in the Pool
    see: https://stackoverflow.com/a/100146
    """
    logger = logging.getLogger(__name__)

    def __init__(cls, name, bases, attrs):
        super(StageType, cls).__init__(name, bases, attrs)
        Pool.register_stage(cls)
        StageType.logger.debug(f"Registering Stage {name}")


class TaskType(ABCMeta):
    """
    Automatically register a class with __metaclass__ = TaskType in the Pool
    see: https://stackoverflow.com/a/100146
    """
    logger = logging.getLogger(__name__)

    def __init__(cls, name, bases, attrs):
        super(TaskType, cls).__init__(name, bases, attrs)
        Pool.register_task(cls)
        StageType.logger.debug(f"Registering Task {name}")
