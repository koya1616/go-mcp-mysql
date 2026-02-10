COMPOSE := docker compose
IMAGE := mcp-mysql:local

.PHONY: help up down restart logs ps build build-prod run-prod

help:
	@echo "Targets:"
	@echo "  make up         - Start dev container in background"
	@echo "  make down       - Stop and remove containers"
	@echo "  make restart    - Restart app container"
	@echo "  make logs       - Tail compose logs"
	@echo "  make ps         - Show compose status"
	@echo "  make build      - Build dev compose image"
	@echo "  make build-prod - Build production image"
	@echo "  make run-prod   - Run production image with current env"

up:
	$(COMPOSE) up -d --build

down:
	$(COMPOSE) down

restart:
	$(COMPOSE) restart app

logs:
	$(COMPOSE) logs -f --tail=100

ps:
	$(COMPOSE) ps

build:
	$(COMPOSE) build

build-prod:
	docker build --target prod -t $(IMAGE) .

run-prod:
	docker run --rm --env-file .env $(IMAGE)
