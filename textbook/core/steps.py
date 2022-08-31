from core import PipelineElement


class SerialStep(PipelineElement):
    def __init__(self, name):
        super().__init__(name, "SerialStep")
        self.report['strategy'] = "serial"

    def execute(self):
        for element in self.pipeline_elements:
            element.execute()


class ParallelStep(PipelineElement):
    pass