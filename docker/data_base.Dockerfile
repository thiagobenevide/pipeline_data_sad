FROM postgres:18

LABEL project="pipeline-sad" \
      environment="producao" \
      database="bcbdb"

COPY backup/bcbdb /tmp/bcbdb.dump
COPY docker/init-db.sh /docker-entrypoint-initdb.d/01_init-db.sh

RUN chmod +x /docker-entrypoint-initdb.d/01_init-db.sh

VOLUME ["/var/lib/postgresql/data"]

EXPOSE 5432
