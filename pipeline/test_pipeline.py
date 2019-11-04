from unittest import TestCase
from .pipeline import FileExists, Initiate
import luigi


class PipelineModellingTest(TestCase):
    """
    """

    def setUp(self):
        luigi.build([FileExists(input_file="./source_data/titanic.csv"),Initiate(input_file="./source_data/titanic.csv")], local_scheduler=True)

    def test_1(self):
        pass
