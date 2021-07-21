# MetaQS API

```
git clone git@github.com:openeduhub/metaqs-api.git

cd metaqs-api
```

Setup python virtual environment of your choice (python >= 3.8).

Install dependencies from `requirements.txt`.

Setup Tunnel
`ssh ... -L 9200:127.0.0.1:9200`

```
docker-compose build
docker-compose up -d
```

Visit `localhost:8080/docs` to see Swagger UI.
