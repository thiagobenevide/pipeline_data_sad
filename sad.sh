#!/bin/bash

IMAGE_NAME="pipeline-sad-postgres"
CONTAINER_NAME="pipeline-sad-db"
VOLUME_NAME="pipeline-sad-pgdata"
HOST_PORT=8594
CONTAINER_PORT=5432
WG_IP="10.0.0.1"   # IP da interface WireGuard no VPS

case "$1" in
  -up)
    if ! systemctl is-active --quiet docker; then
      echo "[SAD] ERRO: Docker nao esta rodando. Execute: sudo systemctl start docker"
      exit 1
    fi

    if ! ip link show wg0 > /dev/null 2>&1; then
      echo "[SAD] ERRO: Interface WireGuard wg0 nao esta ativa. Execute: sudo systemctl start wg-quick@wg0"
      exit 1
    fi

    if [ ! -f .env ]; then
      echo "[SAD] ERRO: arquivo .env nao encontrado. Crie-o com POSTGRES_DB, POSTGRES_USER e POSTGRES_PASSWORD."
      exit 1
    fi

    echo "[SAD] Construindo imagem PostgreSQL 18..."
    docker build -t $IMAGE_NAME -f docker/data_base.Dockerfile .

    echo "[SAD] Criando volume persistente '$VOLUME_NAME'..."
    docker volume create $VOLUME_NAME

    echo "[SAD] Iniciando container na porta $HOST_PORT..."

    docker run -d \
      --name $CONTAINER_NAME \
      -p $WG_IP:$HOST_PORT:$CONTAINER_PORT \
      -v $VOLUME_NAME:/var/lib/postgresql \
      --env-file .env \
      --restart unless-stopped \
      $IMAGE_NAME

    echo ""
    echo "[SAD] Container '$CONTAINER_NAME' iniciado com sucesso."
    echo "[SAD] Host  : $WG_IP:$HOST_PORT (somente via WireGuard)"
    echo "[SAD] Banco : $(grep POSTGRES_DB .env | cut -d= -f2)"
    echo "[SAD] Usuario: $(grep POSTGRES_USER .env | cut -d= -f2)"
    ;;

  -stop)
    echo "[SAD] Parando container '$CONTAINER_NAME'..."
    docker stop $CONTAINER_NAME
    echo "[SAD] Container parado. Dados preservados no volume '$VOLUME_NAME'."
    ;;

  -remove)
    echo "[SAD] Removendo container, imagem e volume..."
    docker stop $CONTAINER_NAME 2>/dev/null || true
    docker rm $CONTAINER_NAME 2>/dev/null || true
    docker rmi $IMAGE_NAME 2>/dev/null || true
    docker volume rm $VOLUME_NAME 2>/dev/null || true
    echo "[SAD] Ambiente removido por completo."
    ;;

  *)
    echo "Uso: ./sad.sh {-up|-stop|-remove}"
    echo ""
    echo "  -up      Builda a imagem, cria volume e sobe o container PostgreSQL 18 na porta $HOST_PORT"
    echo "  -stop    Para o container (dados do volume sao preservados)"
    echo "  -remove  Para e remove container, imagem e volume"
    exit 1
    ;;
esac
