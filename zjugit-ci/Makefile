SERVICE_NAME := zju-os-runner

all:
	docker compose up

register:
	# check if config/config.toml exists
	@if [ -f config/config.toml ]; then \
		echo "config/config.toml already exists, please remove it first"; \
		exit 1; \
	fi
	@if [ -z "$(TOKEN)" ]; then \
		echo "Please provide TOKEN, e.g. make register TOKEN=\"<your_token>\""; \
		exit 1; \
	fi
	# register the runner
	docker compose run --rm $(SERVICE_NAME) register \
		--non-interactive \
		--url "https://git.zju.edu.cn/" \
		--token "$(TOKEN)" \
		--executor "docker" \
		--docker-image git.zju.edu.cn:5050/os/tool:latest \
		--description "zju-os-runner"
	make run

run:
	docker compose run --rm $(SERVICE_NAME) run

clean:
	docker compose down -v --remove-orphans
