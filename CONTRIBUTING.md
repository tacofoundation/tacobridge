# Contributing to tacobridge

Contributions are welcome! Every little bit helps, and credit will always be given.

## Types of Contributions

### Report Bugs

Report bugs at https://github.com/tacofoundation/tacobridge/issues

If you are reporting a bug, please include:

- Your operating system name and version
- Python version and relevant package versions (`tacoreader`, `tacotoolbox`)
- Minimal code example to reproduce the bug
- Full error traceback

### Fix Bugs

Look through the GitHub issues for bugs. Anything tagged with `bug` and `help wanted` is open to whoever wants to implement a fix.

### Implement Features

Look through the GitHub issues for features. Anything tagged with `enhancement` and `help wanted` is open to whoever wants to implement it.

### Write Documentation

tacobridge could always use more documentation, whether as part of the official docs, in docstrings, or even on the web in blog posts and articles.

### Submit Feedback

The best way to send feedback is to file an issue at https://github.com/tacofoundation/tacobridge/issues

If you are proposing a new feature:

- Explain in detail how it would work
- Keep the scope as narrow as possible
- Remember that this is a volunteer-driven project

## Getting Started

Ready to contribute? Here's how to set up tacobridge for local development.

### Prerequisites

- Python 3.11+
- [Poetry](https://python-poetry.org/docs/#installation)
- Git

### Setup

1. Fork the repo on GitHub

2. Clone your fork locally:

    ```bash
    git clone git@github.com:YOUR_USERNAME/tacobridge.git
    cd tacobridge
    ```

3. Install dependencies:

    ```bash
    poetry install --with dev
    ```

4. Create a branch for your changes:

    ```bash
    git checkout -b name-of-your-bugfix-or-feature
    ```

### Development Workflow

1. Make your changes

2. Add tests for your functionality in `tests/`

3. Run linting:

    ```bash
    poetry run ruff check tacobridge/
    poetry run ruff format tacobridge/
    ```

4. Run type checking:

    ```bash
    poetry run mypy tacobridge/
    ```

5. Run tests:

    ```bash
    poetry run pytest
    ```

6. Commit your changes:

    ```bash
    git add .
    git commit -m "Brief description of changes"
    git push origin name-of-your-bugfix-or-feature
    ```

7. Submit a pull request through GitHub

## Pull Request Guidelines

Before you submit a pull request, check that it meets these guidelines:

1. The pull request should include tests
2. If the pull request adds functionality, update the docstrings
3. The PR should work for Python 3.11, 3.12, and 3.13