# go-ingestion-api — build, test, and deploy via Docker Compose
#
# Usage:
#   make help
#   make deploy          # build images + start stack
#   make smoke           # hit health/ingest/results through nginx
#   make down            # stop stack

COMPOSE       ?= docker compose
COMPOSE_FILE  ?= docker-compose.yaml
BASE_URL      ?= http://localhost
BIN           ?= bin/api-service
GO            ?= go

.DEFAULT_GOAL := help

.PHONY: help deps test test-e2e build build-local \
	deploy up down restart rebuild status logs logs-api \
	smoke psql clean tidy droplet-bootstrap

help: ## Show available targets
	@awk 'BEGIN {FS = ":.*##"; printf "\nTargets:\n"} \
		/^[a-zA-Z0-9_-]+:.*?##/ { printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2 }' $(MAKEFILE_LIST)
	@echo ""
	@echo "Examples:"
	@echo "  make deploy && make smoke"
	@echo "  make logs-api"
	@echo "  BASE_URL=http://127.0.0.1 make smoke"
	@echo ""

deps: ## Download Go module dependencies
	$(GO) mod download

tidy: ## Tidy go.mod / go.sum
	$(GO) mod tidy

test: ## Run unit tests
	$(GO) test ./internal/plane/ ./internal/controlplane/ ./internal/dataplane/ ./internal/db/ ./internal/external/ -count=1

test-e2e: ## Run embedded-Postgres end-to-end test
	$(GO) test ./internal/e2e/ -count=1 -timeout 180s -v

build-local: ## Build the API binary locally into ./bin
	mkdir -p $(dir $(BIN))
	CGO_ENABLED=0 $(GO) build -o $(BIN) ./cmd/api
	@echo "built $(BIN)"

build: ## Build Docker images (no start)
	$(COMPOSE) -f $(COMPOSE_FILE) build

deploy: up ## Alias for up (build + start stack)

up: ## Build images and start postgres + api + nginx
	$(COMPOSE) -f $(COMPOSE_FILE) up --build -d
	@echo ""
	@echo "Stack is up. Endpoints via nginx:"
	@echo "  GET  $(BASE_URL)/health"
	@echo "  POST $(BASE_URL)/api/v1/ingest"
	@echo "  GET  $(BASE_URL)/api/v1/pool"
	@echo ""
	@echo "Run: make smoke"

rebuild: ## Force rebuild images and recreate containers
	$(COMPOSE) -f $(COMPOSE_FILE) up --build --force-recreate -d

down: ## Stop and remove containers (keeps postgres volume)
	$(COMPOSE) -f $(COMPOSE_FILE) down

restart: ## Restart all services
	$(COMPOSE) -f $(COMPOSE_FILE) restart

status: ## Show compose service status
	$(COMPOSE) -f $(COMPOSE_FILE) ps

logs: ## Tail logs for all services
	$(COMPOSE) -f $(COMPOSE_FILE) logs -f --tail=200

logs-api: ## Tail API (control + data plane) logs
	$(COMPOSE) -f $(COMPOSE_FILE) logs -f --tail=200 api

smoke: ## Smoke-test deployed stack through nginx
	BASE_URL=$(BASE_URL) bash ./scripts/compose-smoke.sh

psql: ## Open psql in the postgres container
	$(COMPOSE) -f $(COMPOSE_FILE) exec postgres \
		psql -U postgres -d ingest_db

clean: ## Stop stack and delete volumes (destructive)
	$(COMPOSE) -f $(COMPOSE_FILE) down -v --remove-orphans
	rm -rf bin/
	@echo "cleaned containers, volumes, and local bin/"

droplet-bootstrap: ## Print one-time DigitalOcean droplet setup commands
	@echo "Run on a fresh Ubuntu droplet:"
	@echo "  sudo apt-get update && sudo apt-get install -y docker.io docker-compose-v2 git rsync curl"
	@echo "  sudo usermod -aG docker \$$USER"
	@echo "  sudo mkdir -p /opt/go-ingestion-api && sudo chown -R \$$USER:\$$USER /opt/go-ingestion-api"
	@echo "  # 512MB droplets need swap for docker compose builds:"
	@echo "  sudo fallocate -l 2G /swapfile && sudo chmod 600 /swapfile && sudo mkswap /swapfile && sudo swapon /swapfile"
	@echo "  echo '/swapfile none swap sw 0 0' | sudo tee -a /etc/fstab"
	@echo "  # Add your GitHub Actions deploy SSH public key to ~/.ssh/authorized_keys"
	@echo "  # Open TCP 80 in the DigitalOcean cloud firewall"
	@echo "Then add GitHub Actions secrets (see .env.example)."
