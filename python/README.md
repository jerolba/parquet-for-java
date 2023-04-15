Build the docker image:

```bash
docker build -t python_parquet .
```

Execute and get the result in a local disk:

```bash
docker run --rm -v $(pwd)/src/test/resources:/home/data python_parquet
```