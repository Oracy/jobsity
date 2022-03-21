# Postgres For Data Source

You can access sql using ip `localhost:5433`, and access on browser using `localhost:8081`

To get postgres container ip, run command below:
```sh
docker inspect -f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' data_source-pgadmin-compose-1
```
