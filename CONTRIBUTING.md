# Contributing

Thank you for considering contributing to this project. We welcome issues, feature requests, and pull requests.

## Guidelines

1. **Open an issue** first to discuss significant changes before opening a PR.
2. **Follow the existing code style** — Ruff, Black, isort, and mypy strict must pass.
3. **Add tests** for new functionality. Maintain or improve the current coverage.
4. **Keep commits small and descriptive** using Conventional Commits:

   ```
   feat: add funnel metrics sliding window
   fix: parse microsecond timestamps correctly
   chore: update dependencies
   docs: add architecture diagram
   ```

## Development Setup

```bash
# Clone and enter the project
git clone https://github.com/suryalionael/streaming-clickstream-pipeline.git
cd streaming-clickstream-pipeline

# Install dependencies
pip install -r requirements.txt

# Run quality checks
make lint
make format
make typecheck
make test

# Run the full stack
docker compose up -d
```

## Code Quality

All contributions must pass:

| Tool | Command | Requirement |
|---|---|---|
| Ruff | `ruff check .` | Zero errors |
| Black | `black --check .` | All files formatted |
| isort | `isort --check-only .` | All imports sorted |
| mypy | `mypy --strict .` | Zero errors |
| pytest | `pytest tests/` | All tests passing |

## Questions?

Open a [GitHub Discussion](https://github.com/suryalionael/streaming-clickstream-pipeline/discussions) for questions and ideas.
