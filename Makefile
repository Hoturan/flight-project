.PHONY: help fmt validate lint test terraform-init terraform-plan terraform-apply terraform-destroy

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-20s %s\n", $$1, $$2}'

fmt: ## Format Terraform and Python
	terraform -chdir=infra/terraform fmt -recursive
	ruff format src tests

validate: ## Validate Terraform configuration
	terraform -chdir=infra/terraform init -backend=false
	terraform -chdir=infra/terraform validate

lint: ## Lint Python
	ruff check src tests

test: ## Run Python tests
	pytest tests -v

terraform-init: ## Initialize Terraform backend
	terraform -chdir=infra/terraform init

terraform-plan: ## Show Terraform plan
	terraform -chdir=infra/terraform plan

terraform-apply: ## Apply Terraform changes
	terraform -chdir=infra/terraform apply -auto-approve

terraform-destroy: ## Destroy all Terraform-managed resources
	terraform -chdir=infra/terraform destroy -auto-approve