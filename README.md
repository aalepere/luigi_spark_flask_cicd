[![CodeFactor](https://www.codefactor.io/repository/github/aalepere/luigi_spark_flask_cicd/badge)](https://www.codefactor.io/repository/github/aalepere/luigi_spark_flask_cicd)


# Introduction
This repos aims to illustrate how to use luigi and PySpark together to create robust pipeline that can be used in production. It also focuses on how to test such pipeline in conjonction with the CI/CD principles.

# Background
## Luigi
Luigi is python package that allows to create data pipelines. Traditionaly when created pipeline, we chain a list of events to end with the required output. 
Luigi packages helps you to build clean datapipeline with out of the box features such as:
- Dependency resolution, in other words ensure that all upstream tasks are correctly exectude before moving the next one. Luigi allows to create clean graph
- Workflow management
- Graph visualisation
- Parallelisation processing

https://github.com/spotify/luigi

## PySpark
PySpark is the Python API for Spark. The PySpark allows us to leverage all the features of Spark through a Python API. 
In this repo, we are mianly using the Machine Learning functionalities of Spark which includes:
- Feature module, a set of data transformation that can be used for feature engineering (replacing missing values, discretisation and others)
- Classification module, set of classification models (logistic regression, random forest ...)

https://spark.apache.org/docs/latest/api/python/pyspark.html

## Testing & CI/CD

# Installation
## Requirements
### Create a virtual environment
First install `virtualenv` on your machine if that it is not already the case:
```shell
pip install virtualenv
```
Then create the virtual env on you local machine:
```shell
virtualenv -v env
```
Finally active your newly created virtual environment:
```shell
source env/bin/activate
```
### Install requirements
Once you activated your virtual enviromnent, you need to install all the Python packages that are required to run such datapipeline:
```shell
pip install -r requirements.txt
```
## Run the pipeline
Below is the command line, you will require to be able to run the pipeline on your local machine:
```shell
cd pipeline
PYTHONPATH='.' luigi --module pipeline Transform --local-scheduler --input-file "../source_data/titanic.csv"
```
