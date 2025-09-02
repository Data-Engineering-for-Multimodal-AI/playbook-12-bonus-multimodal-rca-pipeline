# Contributing to Advanced Data Engineering Pipeline

Thank you for your interest in contributing to the Advanced Data Engineering Pipeline! This document provides guidelines for contributing to the project.

## Table of Contents

1. [Code of Conduct](#code-of-conduct)
2. [Getting Started](#getting-started)
3. [Development Setup](#development-setup)
4. [Contributing Guidelines](#contributing-guidelines)
5. [Pull Request Process](#pull-request-process)
6. [Issue Reporting](#issue-reporting)
7. [Documentation](#documentation)
8. [Testing](#testing)

## Code of Conduct

### Our Pledge

We are committed to making participation in this project a harassment-free experience for everyone, regardless of age, body size, disability, ethnicity, gender identity and expression, level of experience, nationality, personal appearance, race, religion, or sexual identity and orientation.

### Our Standards

**Positive behavior includes:**
- Using welcoming and inclusive language
- Being respectful of differing viewpoints and experiences
- Gracefully accepting constructive criticism
- Focusing on what is best for the community
- Showing empathy towards other community members

**Unacceptable behavior includes:**
- Harassment of any kind
- Publishing others' private information without permission
- Trolling, insulting, or derogatory comments
- Other conduct which could reasonably be considered inappropriate

## Getting Started

### Prerequisites

Before contributing, ensure you have:

1. **Development Environment**
   - Python 3.9+
   - Docker Desktop
   - Git
   - Code editor (VS Code recommended)

2. **Knowledge Areas**
   - Basic understanding of data engineering concepts
   - Familiarity with streaming technologies (Kafka, Flink)
   - Experience with cloud platforms (GCP preferred)
   - Understanding of containerization (Docker, Kubernetes)

### Areas for Contribution

We welcome contributions in several areas:

**Core Pipeline Development**
- Stream processing optimizations
- New data connectors and adapters
- Performance improvements
- Edge computing enhancements

**Infrastructure & DevOps**
- Kubernetes deployment improvements
- CI/CD pipeline enhancements
- Infrastructure as Code (Terraform)
- Monitoring and observability

**Documentation**
- Tutorial improvements
- API documentation
- Architecture diagrams
- Best practices guides

**Testing & Quality**
- Unit test coverage
- Integration testing
- Performance benchmarking
- Security testing

**Examples & Integrations**
- Industry-specific examples
- Third-party integrations
- Custom data processors
- ML model integrations

## Development Setup

### 1. Fork and Clone

```bash
# Fork the repository on GitHub
# Then clone your fork
git clone https://github.com/YOUR_USERNAME/pb-12-mm-endtoendpipeline.git
cd pb-12-mm-endtoendpipeline/resolve-agent-bonus

# Add upstream remote
git remote add upstream https://github.com/Data-Engineering-for-Multimodal-AI/pb-12-mm-endtoendpipeline.git
```

### 2. Set Up Environment

```bash
# Create virtual environment
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
pip install -r requirements-dev.txt  # Development dependencies
```

### 3. Start Development Stack

```bash
# Start local infrastructure
docker-compose up -d

# Verify setup
python cloud_demo.py --step 1
```

### 4. Development Tools

**Recommended VS Code Extensions:**
- Python
- Docker
- Kubernetes
- YAML
- GitLens

**Code Quality Tools:**
```bash
# Install pre-commit hooks
pre-commit install

# Run code formatting
black .
isort .

# Run linting
flake8 .
pylint src/
```

## Contributing Guidelines

### Code Style

**Python Code Standards:**
- Follow PEP 8 style guidelines
- Use Black for code formatting
- Use isort for import sorting
- Add type hints where appropriate
- Include docstrings for all functions and classes

**Example:**
```python
from typing import Dict, List, Optional
import logging

logger = logging.getLogger(__name__)

def process_sensor_data(
    sensor_readings: Dict[str, float],
    threshold: float = 85.0
) -> Optional[Dict[str, Any]]:
    """
    Process sensor readings and detect anomalies.
    
    Args:
        sensor_readings: Dictionary of sensor name to value
        threshold: Temperature threshold for anomaly detection
        
    Returns:
        Dictionary with processed results or None if invalid input
        
    Raises:
        ValueError: If sensor_readings is empty
    """
    if not sensor_readings:
        raise ValueError("Sensor readings cannot be empty")
    
    # Implementation here
    pass
```

**Configuration Standards:**
- Use YAML for configuration files
- Environment variables for secrets
- Clear naming conventions
- Comprehensive comments

### Commit Messages

Use conventional commit format:

```
type(scope): description

[optional body]

[optional footer]
```

**Types:**
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation changes
- `style`: Code style changes
- `refactor`: Code refactoring
- `test`: Adding or updating tests
- `chore`: Maintenance tasks

**Examples:**
```
feat(streaming): add support for Avro schema evolution

Add capability to handle schema evolution in Kafka streams
with backward compatibility for existing consumers.

Closes #123
```

### Branch Naming

Use descriptive branch names:

```
feature/add-anomaly-detection
fix/kafka-connection-retry
docs/update-installation-guide
refactor/simplify-edge-processor
```

## Pull Request Process

### 1. Before Submitting

**Checklist:**
- [ ] Tests pass locally
- [ ] Code follows style guidelines
- [ ] Documentation updated
- [ ] Commit messages follow convention
- [ ] No merge conflicts with main branch

### 2. Pull Request Template

```markdown
## Description
Brief description of changes

## Type of Change
- [ ] Bug fix
- [ ] New feature
- [ ] Documentation update
- [ ] Performance improvement
- [ ] Other (please describe)

## Testing
- [ ] Unit tests added/updated
- [ ] Integration tests pass
- [ ] Manual testing completed

## Checklist
- [ ] Code follows project style guidelines
- [ ] Self-review completed
- [ ] Documentation updated
- [ ] Tests added for new functionality
```

### 3. Review Process

**Review Criteria:**
- Code quality and style
- Test coverage
- Documentation completeness
- Performance impact
- Security considerations

**Timeline:**
- Initial review within 2-3 business days
- Follow-up reviews within 1 business day
- Merge after 2 approvals from maintainers

## Issue Reporting

### Bug Reports

Use the bug report template:

```markdown
**Bug Description**
Clear description of the bug

**Steps to Reproduce**
1. Step one
2. Step two
3. ...

**Expected Behavior**
What should happen

**Actual Behavior**
What actually happens

**Environment**
- OS: [e.g., macOS 12.0]
- Python: [e.g., 3.9.7]
- Docker: [e.g., 20.10.8]

**Additional Context**
Screenshots, logs, or other helpful information
```

### Feature Requests

Use the feature request template:

```markdown
**Feature Description**
Clear description of the proposed feature

**Use Case**
Why is this feature needed?

**Proposed Solution**
How should this feature work?

**Alternatives Considered**
Other approaches you've considered

**Additional Context**
Any other context or screenshots
```

## Documentation

### Types of Documentation

**Code Documentation:**
- Inline comments for complex logic
- Docstrings for all public functions/classes
- Type hints for function parameters and returns

**User Documentation:**
- README updates for new features
- Tutorial additions for new capabilities
- API documentation for public interfaces

**Developer Documentation:**
- Architecture decision records (ADRs)
- Setup and configuration guides
- Troubleshooting guides

### Documentation Standards

**Markdown Guidelines:**
- Use clear headings and structure
- Include code examples where appropriate
- Add diagrams for complex concepts
- Keep language clear and concise

**Code Examples:**
```python
# Good: Clear, commented example
def example_function():
    """Example with clear documentation."""
    # Process data
    result = process_data()
    
    # Return processed result
    return result
```

## Testing

### Testing Strategy

**Unit Tests:**
- Test individual functions and classes
- Mock external dependencies
- Aim for >90% code coverage

**Integration Tests:**
- Test component interactions
- Use test containers for infrastructure
- Validate end-to-end workflows

**Performance Tests:**
- Benchmark critical paths
- Monitor resource usage
- Test scalability limits

### Running Tests

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=src --cov-report=html

# Run specific test categories
pytest -m unit
pytest -m integration
pytest -m performance
```

### Test Guidelines

**Test Structure:**
```python
def test_function_name():
    # Arrange
    input_data = create_test_data()
    
    # Act
    result = function_under_test(input_data)
    
    # Assert
    assert result == expected_result
```

**Test Naming:**
- Use descriptive test names
- Follow pattern: `test_[function]_[scenario]_[expected_result]`
- Example: `test_process_sensor_data_with_high_temp_returns_alert`

## Getting Help

### Community Support

**GitHub Discussions:**
- General questions and discussions
- Feature brainstorming
- Community showcase

**Issues:**
- Bug reports
- Feature requests
- Technical problems

### Maintainer Contact

**Core Team:**
- Primary maintainers listed in README.md
- Response time: 2-3 business days

**Enterprise Support:**
- Contact information in main README
- Commercial support options available

## Recognition

### Contributor Recognition

**Contributors are recognized through:**
- GitHub contributor statistics
- Release notes mentions
- Hall of fame in documentation
- Conference presentation opportunities

**Special Recognition:**
- Outstanding contributions highlighted in quarterly updates
- Speaker opportunities at community events
- Collaboration opportunities with research institutions

---

Thank you for contributing to the Advanced Data Engineering Pipeline! Your contributions help advance the state of manufacturing AI and data engineering for everyone.
