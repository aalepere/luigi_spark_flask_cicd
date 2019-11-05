import os
import shutil
import unittest

import luigi
import pandas as pd

from pipeline import FileExists, Initiate, Model, Transform

output_path = "test_data"
extension = "csv"


class PipelineModellingTest(unittest.TestCase):
    """
    """

    def setUp(self):
        if os.path.exists(output_path):
            shutil.rmtree(output_path)

        luigi.build(
            [
                FileExists(input_file="test_data.csv"),
                Initiate(input_file="test_data.csv", output_path=output_path),
                Transform(input_file="test_data.csv", output_path=output_path),
                Model(input_file="test_data.csv", output_path=output_path),
            ],
            local_scheduler=True,
            detailed_summary=True,
        )

    def test_split_train_test(self):
        """ Ensure that the initial data set is correctly split into 70/30 train and test sets"""
        df_orig = pd.read_csv("test_data.csv")
        test_files = os.listdir(output_path + "/data_initiated_test.csv")
        for test_file in test_files:
            if test_file.endswith(".csv"):
                df_initiated_test = pd.read_csv(output_path + "/data_initiated_test.csv/" + test_file)
        train_files = os.listdir(output_path + "/data_initiated.csv")
        for train_file in train_files:
            if train_file.endswith(".csv"):
                df_initiated_train = pd.read_csv(output_path + "/data_initiated.csv/" + train_file)

        self.assertEqual(df_orig.shape[0], 7)
        self.assertEqual(df_initiated_test.shape[0], 2)
        self.assertEqual(df_initiated_train.shape[0], 5)


if __name__ == "__main__":
    unittest.main()
