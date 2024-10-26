#####
# BGPDATA - BGP Data Collection and Analytics Service
# 
# This software is part of the BGPDATA project, which is designed to collect, process, and analyze BGP data from various sources.
# It helps researchers and network operators get insights into their network by providing a scalable and reliable way to analyze and inspect historical and live BGP data from RIPE NCC RIS.
# 
# Author: Robin Röper
# 
# © 2024 BGPDATA. All rights reserved.
# 
# Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:
# 1. Redistributions of source code must retain the above copyright notice, this list of conditions, and the following disclaimer.
# 2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions, and the following disclaimer in the documentation and/or other materials provided with the distribution.
# 3. Neither the name of BGPDATA nor the names of its contributors may be used to endorse or promote products derived from this software without specific prior written permission.
# 
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#####

# Variables
DC = docker compose
WEB_SERVICE = core-web

.PHONY: help up down build restart logs ps test lint format clean migrate exec prune

.DEFAULT_GOAL := up

help: ## Show help message
	@echo "Usage: make [target]"
	@echo ""
	@echo "Targets:"
	@awk 'BEGIN {FS = ":.*##"} /^[a-zA-Z0-9_-]+:.*##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 }' $(MAKEFILE_LIST)

up: ## Start the application in detached mode (rebuilding images if necessary)
	$(DC) up

down: ## Stop the application and remove containers, networks, and volumes
	$(DC) down --remove-orphans

build: ## Build the Docker images
	$(DC) build

restart: ## Restart the application
	$(DC) restart

logs: ## View application logs
	$(DC) logs -f

ps: ## List running services
	$(DC) ps

test: ## Run tests
	$(DC) exec $(WEB_SERVICE) pytest tests/

lint: ## Run linters and static analysis
	$(DC) exec $(WEB_SERVICE) flake8 .
	$(DC) exec $(WEB_SERVICE) mypy .

format: ## Format code using Black
	$(DC) exec $(WEB_SERVICE) black .

clean: ## Remove all files ignored by git
	git clean -fdX

migrate: ## Run database migrations
	$(DC) exec $(WEB_SERVICE) alembic upgrade head

exec: ## Execute a command in a running container. Usage: make exec SERVICE=<service> CMD="<command>"
ifndef SERVICE
	@echo "Error: SERVICE variable is not set. Usage: make exec SERVICE=<service> CMD=\"<command>\""
	@exit 1
endif
ifndef CMD
	@echo "Error: CMD variable is not set. Usage: make exec SERVICE=<service> CMD=\"<command>\""
	@exit 1
endif
	$(DC) exec $(SERVICE) sh -c '$(CMD)'

prune: ## Remove unused Docker images and volumes
	docker system prune -f
	docker volume prune -f
