COMPOSE = docker compose -f infra/docker-compose.yml

.PHONY: up down ps logs build restart psql topic-list

up:
	$(COMPOSE) up --build -d

down:
	$(COMPOSE) down -v

ps:
	$(COMPOSE) ps

logs:
	$(COMPOSE) logs -f --tail=200

build:
	$(COMPOSE) build --no-cache

restart:
	$(COMPOSE) restart

psql:
	docker exec -it fraud-postgres psql -U fraud -d fraud_db

topic-list:
	docker exec -it fraud-redpanda rpk topic list --brokers redpanda:9092
