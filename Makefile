help: ## Show this
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

clean: ## Find and delete all cached .pyc and .pyo files
	find . -type f -name '*.py[co]' -delete -o -type d -name __pycache__ -delete

dagster_conf_local: ## Create local dagster config
	mkdir -p $DAGSTER_HOME && cp template/dagster-conf.yaml $DAGSTER_HOME/dagster.yaml