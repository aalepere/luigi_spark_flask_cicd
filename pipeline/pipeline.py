import math

import luigi
import luigi.contrib.spark
from pyspark import SparkContext
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import Bucketizer, Imputer, StringIndexer, VectorAssembler
from pyspark.sql import SQLContext


class FileExists(luigi.Task):
    """ Check if the file exists in the local file system.
    """

    # The location and file name of the dataset is passed as a parameter
    # to the pipeline
    input_file = luigi.Parameter()

    def output(self):
        """Saves the dataset locally"""
        return luigi.LocalTarget(str(self.input_file))

    def run(self):
        """ Open the file passed as parameter; if the file doesnt exist this
            will fail the FileExists luigi task. """
        open(str(self.input_file))


class Initiate(luigi.contrib.spark.PySparkTask):
    """ 1) Load the file into a spark frame
        2) Convert non-numeric fields into numeric with StrinIndexer
        3) Split the orginal file in 30% test and 70% training
        4) Save the outputs into 2 different files
    """

    # Keep input_file parameter as used in FileExists
    input_file = luigi.Parameter()
    output_path = luigi.Parameter()

    def requires(self):
        """This task requires that the input file exists"""
        return [FileExists(self.input_file)]

    def output(self):
        """ Two initiated outputs are saved separetly: train and test, however they are manipulated the same
            way to ensure consistency in the overall pipeline
        """
        return {
            "test": luigi.LocalTarget(self.output_path + "/data_initiated_test.csv"),
            "train": luigi.LocalTarget(self.output_path + "/data_initiated.csv"),
        }

    def main(self, sc, *args):
        """ Run all initiate transformation:
            - string features to numeric features
            - split of train/test
        """
        sqlContext = SQLContext(sc)
        # Read csv file
        df = sqlContext.read.csv(self.input_file, sep="\t", header=True, inferSchema=True)

        # Convert string feature Sex to a numeric value
        col = "Sex"
        indexer = StringIndexer(inputCol=col, outputCol="{}_indexed".format(col))
        df = indexer.fit(df).transform(df)

        # Save results
        train, test = df.randomSplit([0.7, 0.3], seed=12345)
        test.write.csv(self.output()["test"].path, header=True)
        train.write.csv(self.output()["train"].path, header=True)


class Transform(luigi.contrib.spark.PySparkTask):
    """ Transform the features before pushing to model fitting:
        1) Keep only features required
        2) Replace missing values
        3) Discretize conntinous features
    """

    # Keep input_file parameter as used in FileExists
    input_file = luigi.Parameter()
    output_path = luigi.Parameter()

    def requires(self):
        """Requires both test and train to be initated"""
        return [Initiate(self.input_file, self.output_path)]

    def output(self):
        """ Two transformned outputs are saved separetly: train and test, however they are manipulated the same
            way to ensure consistency in the overall pipeline
        """
        return {
            "test": luigi.LocalTarget(self.output_path + "/data_transformed_test.csv"),
            "train": luigi.LocalTarget(self.output_path + "/data_transformed.csv"),
        }

    def main(self, sc, *args):
        """ For each input files, i.e. train and test 'initiated, apply the same set of transformatons
        """

        sqlContext = SQLContext(sc)
        # For each key in the output dictionary of the Initiate task, i.e. train and test
        for inputFile in Initiate(self.input_file, self.output_path).output():
            df = sqlContext.read.csv(
                Initiate(self.input_file, self.output_path).output()[inputFile].path, sep=",", header=True, inferSchema=True
            )

            # Select final list of features
            list_features = ["Age", "Sex_indexed", "Fare", "Survived"]
            df = df.select(*list_features)

            # Replace missing values
            cols_missing = ["Age"]
            for col in cols_missing:
                imputer = Imputer(inputCols=[col], outputCols=["{}_replace_missings".format(col)]).setMissingValue(26.0)
                df = imputer.fit(df).transform(df)

            # Discretize
            cols_disc = {
                "Age_replace_missings": [-math.inf, 0.83, 21.0, 26.0, 33.0, 71.0, math.inf],
                "Fare": [-math.inf, 7.225, 8.122, 26.0, 83.475, math.inf],
            }
            for col in cols_disc:
                bucketizer = Bucketizer(splits=cols_disc[col], inputCol=col, outputCol="{}_discretized".format(col))
                df = bucketizer.transform(df)

            df.write.csv(self.output()[inputFile].path, header=True)


class Model(luigi.contrib.spark.PySparkTask):
    """ Build a Logistic Regression based on the train transformed data and then evaluates its
    performance with the test transformed data """

    # Keep input_file parameter as used in FileExists
    input_file = luigi.Parameter()
    output_path = luigi.Parameter()

    def requires(self):
        """ Ensure that the data has been transformed """
        return [Transform(self.input_file, self.output_path)]

    def output(self):
        """ XXX """

    def main(self, sc, *args):
        """ Load train & test, fit the logistic regression and then evaluate """

        sqlContext = SQLContext(sc)
        # Load training file
        df = sqlContext.read.csv(
            Transform(self.input_file, self.output_path).output()["train"].path, sep=",", header=True, inferSchema=True
        )

        # Convert all features into a vectors call features
        assembler = VectorAssembler(
            inputCols=["Age_replace_missings_discretized", "Fare_discretized", "Sex_indexed"], outputCol="features"
        )
        output = assembler.transform(df)

        # Fit logistic regression
        lr = LogisticRegression(featuresCol="features", labelCol="Survived", maxIter=10)
        lrModel = lr.fit(output)

        # Print the coefficients and intercept for logistic regression
        print("Coefficients: " + str(lrModel.coefficients))
        print("Intercept: " + str(lrModel.intercept))


if __name__ == "__main__":
    sc = SparkContext("local", "Spark Pipeline")
    sqlContext = SQLContext(sc)
    luigi.run()
