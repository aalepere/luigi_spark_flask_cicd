import unittest
from pipeline import FileExists, Initiate, Transform, Model
import luigi


class PipelineModellingTest(unittest.TestCase):
    """
    """

    def setUp(self):
        luigi.build(
            [
                FileExists(input_file="../source_data/titanic.csv"),
                Initiate(input_file="../source_data/titanic.csv"),
                Transform(input_file="../source_data/titanic.csv"),
                Model(input_file="../source_data/titanic.csv"),
            ],
            local_scheduler=True,
            detailed_summary=True,
        )

    def test_1(self):
        pass


if __name__ == "__main__":
    unittest.main()
