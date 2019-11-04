[![CodeFactor](https://www.codefactor.io/repository/github/aalepere/luigi_spark_flask_cicd/badge)](https://www.codefactor.io/repository/github/aalepere/luigi_spark_flask_cicd)


# luigi_spark_flask_cicd
Create machine learning data pipelines ready for production

# Commamd to run
```shell
PYTHONPATH='.' luigi --module pipeline Transform --local-scheduler --input-file "../source_data/titanic.csv"
```
