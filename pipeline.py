import luigi
import luigi.contrib.spark
from pyspark import SparkContext
from pyspark.ml.feature import StringIndexer
from pyspark.sql import SQLContext


class FileExistsTask(luigi.Task):

    input_file = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(str(self.input_file))

    def run(self):
        fid = open(str(self.input_file))


class Initiate(luigi.contrib.spark.PySparkTask):

    input_file = luigi.Parameter()
    test_flg = luigi.BoolParameter()

    def requires(self):
        # this task requires that the input file exists
        return [FileExistsTask(self.input_file)]

    def output(self):

        if test_flg:
            return luigi.LocalTarget("pipeline_data/data_iniated_test.csv")
        else:
            return luigi.LocalTarget("pipeline_data/data_initiated.csv")

    def run(self):

        # Read csv file
        df = sqlContext.read.csv(
            self.input_file, sep="\t", header=True, inferSchema=True
        )

        # Convert string feature Sex to a numeric value
        col = "Sex"
        indexer = StringIndexer(inputCol=col, outputCol="{}_indexed".format(col))
        df = indexer.fit(df).transform(df)

        # Save results
        df.write.csv(self.output().path, header=True)


class Transform(luigi.contrib.spark.PySparkTask):

    input_file = luigi.Parameter()

    def requires(self):
        return [Initiate(self.input_file)]

    def output(self):
        return luigi.LocalTarget("pipeline_data/data_transformed.csv")

    def run(self):
        df = sqlContext.read.csv(
            Initiate(self.input_file).output().path,
            sep=",",
            header=True,
            inferSchema=True,
        )
        list_features = ["Age", "Sex_indexed", "Fare", "Survived"]
        df = df.select(*list_features)
        df.write.csv(self.output().path, header=True)


if __name__ == "__main__":
    sc = SparkContext("local", "Spark Pipeline")
    sqlContext = SQLContext(sc)
    luigi.run()
