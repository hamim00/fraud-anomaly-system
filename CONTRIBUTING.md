# Contributing to Fraud Detection System

First off, thank you for considering contributing to this project! 🎉

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [How Can I Contribute?](#how-can-i-contribute)
- [Development Setup](#development-setup)
- [Coding Standards](#coding-standards)
- [Commit Messages](#commit-messages)
- [Pull Request Process](#pull-request-process)

## Code of Conduct

This project and everyone participating in it is governed by our commitment to fostering an open and welcoming environment. Please be respectful and constructive in all interactions.

## How Can I Contribute?

### Reporting Bugs

Before creating bug reports, please check existing issues to avoid duplicates. When creating a bug report, include:

- **Clear title** describing the issue
- **Steps to reproduce** the behavior
- **Expected behavior** vs actual behavior
- **Environment details** (OS, Docker version, Python version)
- **Logs** if applicable

### Suggesting Enhancements

Enhancement suggestions are welcome! Please include:

- **Clear title** describing the enhancement
- **Detailed description** of the proposed functionality
- **Use case** explaining why this would be useful
- **Possible implementation** if you have ideas

### Code Contributions

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Write/update tests
5. Submit a pull request

## Development Setup

### Prerequisites

- Docker 20.10+
- Docker Compose 2.0+
- Python 3.11+
- Git

### Local Development

```bash
# Clone your fork
git clone https://github.com/yourusername/fraud-detection-system.git
cd fraud-detection-system

# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # Linux/Mac
# or
.venv\Scripts\activate     # Windows

# Install development dependencies
pip install -r requirements-dev.txt

# Start infrastructure
cd infra
docker compose up -d redpanda postgres

# Run tests
pytest tests/
```

### Running Individual Services

```bash
# Run a service locally (outside Docker)
cd services/model_service
pip install -r requirements.txt
python app.py
```

## Coding Standards

### Python Style

- Follow [PEP 8](https://pep8.org/) style guide
- Use [Black](https://github.com/psf/black) for code formatting
- Use [isort](https://pycqa.github.io/isort/) for import sorting
- Maximum line length: 88 characters (Black default)

```bash
# Format code
black services/
isort services/

# Check linting
flake8 services/
```

### Type Hints

Use type hints for function signatures:

```python
def calculate_features(
    transaction_id: str,
    amount: float,
    timestamp: datetime
) -> dict[str, Any]:
    ...
```

### Docstrings

Use Google-style docstrings:

```python
def score_transaction(transaction_id: str) -> ScoringResult:
    """Score a transaction for fraud probability.
    
    Args:
        transaction_id: Unique identifier for the transaction.
        
    Returns:
        ScoringResult containing fraud probability and decision.
        
    Raises:
        TransactionNotFoundError: If transaction doesn't exist.
    """
    ...
```

### Testing

- Write tests for new functionality
- Maintain test coverage above 80%
- Use pytest fixtures for setup/teardown

```python
# tests/test_scoring.py
import pytest
from model_service.scoring import score_transaction

@pytest.fixture
def sample_transaction():
    return {"transaction_id": "test-123", "amount": 100.0}

def test_score_returns_valid_decision(sample_transaction):
    result = score_transaction(sample_transaction["transaction_id"])
    assert result.decision in ["APPROVE", "REVIEW", "BLOCK"]
```

## Commit Messages

Follow [Conventional Commits](https://www.conventionalcommits.org/):

```
<type>(<scope>): <description>

[optional body]

[optional footer]
```

### Types

- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation only
- `style`: Code style (formatting, no code change)
- `refactor`: Code refactoring
- `test`: Adding/updating tests
- `chore`: Maintenance tasks

### Examples

```
feat(model-service): add batch scoring endpoint

fix(drift-detector): handle empty reference data gracefully

docs(readme): add API documentation section

test(feature-consumer): add unit tests for velocity features
```

## Pull Request Process

1. **Update documentation** if you're changing functionality
2. **Add tests** for new features
3. **Ensure all tests pass** locally
4. **Update CHANGELOG.md** with your changes
5. **Request review** from maintainers

### PR Checklist

- [ ] Code follows project style guidelines
- [ ] Tests added/updated and passing
- [ ] Documentation updated
- [ ] Commit messages follow convention
- [ ] PR description explains changes clearly

### Review Process

1. Maintainer will review within 3-5 business days
2. Address any requested changes
3. Once approved, maintainer will merge

---

Thank you for contributing! 🙌
