set shell := ["bash", "-eu", "-o", "pipefail", "-c"]

files_to_fmt := env_var_or_default('files_to_fmt', 'src tests main.py')
files_to_check := env_var_or_default('files_to_check', 'src tests main.py')

# Format all
fmt: format

format: remove_imports isort black

# Check code quality
lint: check

# Mirror Makefile: currently only mypy
check: mypy flake8

# Remove unused imports
remove_imports:
  @autoflake -ir --remove-unused-variables --ignore-init-module-imports --remove-all-unused-imports {{files_to_fmt}}

# Sort imports
isort:
  @isort {{files_to_fmt}}

# Format code
black:
  @black {{files_to_fmt}}

# Check pep8
flake8:
  @flake8 {{files_to_check}}

# Check typing
mypy:
  @mypy {{files_to_check}}
