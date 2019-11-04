[![CodeFactor](https://www.codefactor.io/repository/github/aalepere/luigi_spark_flask_cicd/badge)](https://www.codefactor.io/repository/github/aalepere/luigi_spark_flask_cicd)

[![Actions Status](https://github.com/aalepere/luigi_spark_flask_cicd/workflows/pythonapp/badge.svg)](https://github.com/aalepere/luigi_spark_flask_cicd/actions)

# luigi_spark_flask_cicd
Create machine learning data pipelines ready for production

# Commamd to run
PYTHONPATH='.' luigi --module pipeline Transform --local-scheduler --input-file "../source_data/titanic.csv"
