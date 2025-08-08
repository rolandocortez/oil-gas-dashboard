.PHONY: setup install run docker build lint

setup:
\tcp -n .env.example .env || true
\tpip install -r requirements.txt

install:
\tpip install -r requirements.txt

run:
\tbash run_pipeline.sh

docker:
\tbash run_docker.sh

build:
\tdocker build -t oil-gas-dashboard .

lint:
\tpre-commit run --all-files || true
