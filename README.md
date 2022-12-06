# kafka-project

# Spin up Kafka and Zookeper
```shell
docker-compose up -d

# as there was an issue with the dockerized application I had to run locally
python3 main.py
```

## Potential improvements
* add unit tests for functions
  * convert timezone function
  * ...
* further isolation of functions / code logic
* handle potential exceptions
* docstrings for functions to increase maintainability for other developers
* add types for functions to ensure typesafety
* add a config and move parameters there
* use logger instead of print statements
* embed schema-registry to ensure schema compatibilities
* add library versions to requirements file