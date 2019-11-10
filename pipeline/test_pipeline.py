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
        Unit test for each transformations of the pipeline
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

        self.df_orig = pd.read_csv("test_data.csv")
        test_files = os.listdir(output_path + "/data_initiated_test.csv")
        for test_file in test_files:
            if test_file.endswith(".csv"):
                self.df_initiated_test = pd.read_csv(output_path + "/data_initiated_test.csv/" + test_file)
        train_files = os.listdir(output_path + "/data_initiated.csv")
        for train_file in train_files:
            if train_file.endswith(".csv"):
                self.df_initiated_train = pd.read_csv(output_path + "/data_initiated.csv/" + train_file)

    def test_split_train_test(self):
        """ Ensure that the initial data set is correctly split into 70/30 train and test sets"""

        self.assertEqual(self.df_orig.shape[0], 7)
        self.assertEqual(self.df_initiated_test.shape[0], 2)
        self.assertEqual(self.df_initiated_train.shape[0], 5)

    def test_select_columns_initiated(self):
        """ Check the columns in the dataframe at the end of the initiation process """

        self.assertEqual(
            list(self.df_initiated_test.columns),
            [
                "PassengerId",
                "Survived",
                "Pclass",
                "Name",
                "Sex",
                "Age",
                "SibSp",
                "Parch",
                "Ticket",
                "Fare",
                "Cabin",
                "Embarked",
                "Sex_indexed"
            ]
        )

    def test_sex_indexation(self):
        """ Check that the Male/Female are correctly transformed to 0/1 respectively """

        male = self.df_initiated_train[self.df_initiated_train["PassengerId"] == 1][["Sex", "Sex_indexed"]]
        female = self.df_initiated_train[self.df_initiated_train["PassengerId"] == 2][["Sex", "Sex_indexed"]]
        print(female)
        self.assertEqual(male["Sex"][0], "male")
        self.assertEqual(male["Sex_indexed"][0], 0.0)
        self.assertEqual(female["Sex"][1], "female")
        self.assertEqual(female["Sex_indexed"][1], 1.0)


if __name__ == "__main__":
    unittest.main()
