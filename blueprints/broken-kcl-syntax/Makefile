.PHONY: help run
.DEFAULT_GOAL := help

KCL := kcl

help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

run: ## Run the broken blueprint — KCL should exit non-zero with a syntax error
	$(KCL) run main.k; echo "exit code: $$?"
