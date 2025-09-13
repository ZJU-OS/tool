SERVICE_NAME := zju-os-runner

.PHONY: all clean register

all:
	@if [ ! -f config/config.toml ]; then \
		echo "config/config.toml not found, please run 'make register TOKEN=\"<your_token>\"' first"; \
		exit 1; \
	fi
	docker compose up -d

register:
	@if [ -f config/config.toml ]; then \
		echo "config/config.toml already exists, please remove it first"; \
		exit 1; \
	fi
	@if [ -z "$(TOKEN)" ]; then \
		echo "Please provide TOKEN, e.g. make register TOKEN=\"<your_token>\""; \
		exit 1; \
	fi
	docker compose run --rm $(SERVICE_NAME) register \
		--non-interactive \
		--url "https://git.zju.edu.cn/" \
		--token "$(TOKEN)" \
		--executor "docker" \
		--docker-image git.zju.edu.cn:5050/os/tool:latest \
		--description "zju-os-runner"

clean:
	docker compose down -v --remove-orphans
