import logging
import json
import traceback
from abc import ABCMeta, abstractmethod
from collections import OrderedDict
from core.exceptions import PreConditionNotSatisfiedError


class Pool:
    """
    A collection of tasks and/or stages.
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
        Pool.stages.append(stage)
        Pool.logger.debug("Registered %s in task pool", stage.name)

    @staticmethod
    def get_all_tasks():
        """
        :return: return all tasks in the pool
        :rtype: list[PipelineElement]
        """
        return Pool.tasks

    @staticmethod
    def get_all_stages():
        """return all stages in the pool

        :return: _description_
        :rtype: list[Stage]
        """
        return Pool.stages


class PipelineElement:
    """
    An executable and composable entity that represents the test pipeline and provides
    functions to execute and log events from the test pipeline.
    """

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
        """Appends a pipeline element to the execution pipeline

        :param pipeline_element: A PipelineElement object to be appended to the pipeline
        :type pipeline_element: PipelineElement
        """
        self.pipeline_elements.append(pipeline_element)

    def extend_pipeline(self, pipeline_elements):
        """
        A utility function to extend the pipeline with a list of pipeline elements.

        :param pipeline_elements: list of pipeline elements
        :type pipeline_elements: list[PipelineElement]
        """
        self.pipeline_elements.extend(pipeline_elements)


    def pre_condition(self):
        """
        Override if certain checks need to be performed before executing the pipeline.
        Copy artifacts etc.
        """
        raise NotImplementedError


    def pre_condition_handler(self):
        """Executes the pre-condition and handles the output and report generation

        :return: a boolean indicationg whether the execution should continue or not
        :rtype: bool
        """
        return_status = False
        try:
            self.pre_condition()
            return_status = True
        except PreConditionNotSatisfiedError as err:
            elements = ", ".join([element.name for element in self.pipeline_elements])
            if self.hard_error_pre_condition:
                self.logger.error("The pre condition check for %s %s was not satisfied. "
                                  "Therefore, the execution of the following pipeline elements is "
                                  "being skipped: %s", self.type, self.name, elements)
                self.report["result"] = "exec_fail"
                self.exit_code = 1
            else:
                self.logger.warning("The pre condition check for %s %s was not satisfied. "
                                    "However, we are continuing the execution of the tasks", self.type, self.name)

                self.exit_code = 4  # pre_condition failed
                return_status = True
            self.logger.info("Exception info: %s", err.message, exc_info=True)
            self.report["error"] = err.message
            self.report["trace"] = traceback.format_exc()
            self.report["exit_code"] = self.exit_code
        except NotImplementedError:
            self.logger.warning("The pre condition check for %s %s was not defined. "
                                    "However, we are continuing the execution of the tasks", self.type, self.name)

            self.exit_code = 5  # pre_condition not defined
            return_status = True
            self.report["error"] = "pre condition not defined"
            self.report["trace"] = traceback.format_exc()
            self.report["exit_code"] = self.exit_code

        return return_status

    def run(self):
        """_summary_ executes the child pipeline elements"""
        for element in self.pipeline_elements:
            element.execute()

    def execute(self):
        """"_summary_ executes the pipeline element"""
        if not self.pre_condition_handler():
            self.logger.error("Pre condition failed for %s: %s", self.type, self.name)
            return
        self.logger.info("Executing Pipeline for %s %s", self.type, self.name)
        pipeline_elements_csv = ', '.join(
            [element.name for element in self.pipeline_elements])
        self.logger.info("%s %s has the following pipeline elements registered: %s", self.type, self.name, pipeline_elements_csv)
        self.run()
        # Update report
        self.post_process()

    def post_process(self):
        """ _summary_ Generate Report and update Exit Code """
        pipeline_reports = { element.name: element.report for element in self.pipeline_elements }
        self.report['total_elements'] = len(pipeline_reports)
        self.report['element_reports'] = pipeline_reports
        exit_codes = set([test.exit_code for test in self.pipeline_elements])
        if self.exit_code == 4:
            self.report["result"] = "pre condition failed! "
        if 1 in exit_codes:
            self.exit_code = 1
            # switch to codes someday
            self.report["result"] += "some or all tasks fail"
        elif 3 in exit_codes:
            self.exit_code = 3
            self.report["result"] += "warnings present"
        else:
            self.report["result"] += "all tasks passed"


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
        TaskType.logger.debug(f"Registering Task {name}")
