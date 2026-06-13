FROM postgres:18

LABEL project="pipeline-sad" \
      environment="producao" \
      database="bcbdb"

COPY backup/backup_sad_20260610 /tmp/backup_sad_20260610.dump
COPY docker/init-db.sh /docker-entrypoint-initdb.d/01_init-db.sh

RUN chmod +x /docker-entrypoint-initdb.d/01_init-db.sh

VOLUME ["/var/lib/postgresql"]

EXPOSE 5432
