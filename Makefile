.PHONY: reset clean init-db create-user build up all

# Stop all containers, remove volumes and orphans
reset:
	docker-compose down -v
	docker-compose down --remove-orphans

# Initialize Airflow DB
init-db:
	docker-compose run airflow-webserver airflow db init

# Create default admin user
create-user:
	docker-compose run airflow-webserver airflow users create \
		--username admin \
		--firstname Admin \
		--lastname User \
		--role Admin \
		--email admin@example.com \
		--password admin

# Build all images
build:
	docker-compose build

# Bring containers up in detached mode
up:
	docker-compose up -d

# Run full setup step-by-step
all: reset init-db create-user build up
