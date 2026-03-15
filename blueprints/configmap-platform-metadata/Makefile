.PHONY: help run run-resources run-json fmt lint package
.DEFAULT_GOAL := help

KCL     := kcl
FIXTURE ?= tests/fixtures/basic.yaml
DIST    ?= dist

help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

run: ## Run blueprint with fixture — full output: context + resources (YAML)
	@ctx=$$(python3 -c 'import json,yaml; print(json.dumps(yaml.safe_load(open("$(FIXTURE)"))))'); \
	$(KCL) run main.k -D "context=$$ctx"

run-resources: ## Run blueprint — resources only (YAML)
	@ctx=$$(python3 -c 'import json,yaml; print(json.dumps(yaml.safe_load(open("$(FIXTURE)"))))'); \
	$(KCL) run main.k -D "context=$$ctx" -S resources

run-json: ## Run blueprint — full output as JSON, pipe through jq for filtering
	@ctx=$$(python3 -c 'import json,yaml; print(json.dumps(yaml.safe_load(open("$(FIXTURE)"))))'); \
	$(KCL) run main.k -D "context=$$ctx" --format json

fmt: ## Format all KCL source files in place
	$(KCL) fmt main.k schemas/

lint: ## Check KCL syntax and schemas
	@ctx=$$(python3 -c 'import json,yaml; print(json.dumps(yaml.safe_load(open("$(FIXTURE)"))))'); \
	$(KCL) run main.k -D "context=$$ctx" > /dev/null && echo "OK"

package: ## Package blueprint into a tar artifact under dist/
	@mkdir -p $(DIST)
	$(KCL) mod pkg --target $(DIST)
	@echo "Packaged: $$(ls $(DIST)/*.tar)"
