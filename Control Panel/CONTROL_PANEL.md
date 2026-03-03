# Control Panel Configuration

This file provides an overview of all the skills configured for the Lien Automation v2 project and how to use them.

## Available Skills

### 1. check-compiler-errors
**Purpose**: Run compile and type-check commands and report failures
**Context File**: [check-compiler-errors-context.md](check-compiler-errors-context.md)
**When to Use**: When you encounter TypeScript compilation errors or want to validate code quality
**Commands**: `npm run test:types`, `npm run build`

### 2. run-smoke-tests
**Purpose**: Run smoke tests, debug failures, and verify fixes
**Context File**: [run-smoke-tests-context.md](run-smoke-tests-context.md)
**When to Use**: Before committing changes or when tests are failing
**Commands**: `npm test`, `npm run test:smoke`, `npm run test:ca-sos-range`

### 3. new-branch-and-pr
**Purpose**: Create a fresh branch, complete work, and open a pull request
**Context File**: [new-branch-and-pr-context.md](new-branch-and-pr-context.md)
**When to Use**: When starting new feature work or bug fixes
**Process**: Branch creation → Implementation → Testing → Commit → PR

### 4. fix-ci
**Purpose**: Find failing CI jobs, inspect logs, and apply focused fixes
**Context File**: [fix-ci-context.md](fix-ci-context.md)
**When to Use**: When GitHub Actions CI builds are failing
**Process**: Identify failure → Debug locally → Apply fix → Verify in CI

## Usage Guidelines

1. **Always run tests locally** before pushing changes
2. **Check compiler errors** regularly to maintain code quality
3. **Use feature branches** for all non-trivial changes
4. **Monitor CI status** and fix failures promptly
5. **Follow commit conventions** for clear history

## Environment Setup

Ensure you have the following configured:
- Node.js >= 20.16.0
- npm dependencies installed (`npm install`)
- Environment variables in `.env` file
- Docker installed (for containerized testing)
- Access to Bright Data Scraping Browser
- Google Sheets API credentials

## Skill Integration

These skills work together to provide a comprehensive development workflow:
1. Start with `new-branch-and-pr` for new work
2. Use `check-compiler-errors` during development
3. Run `run-smoke-tests` before committing
4. Use `fix-ci` if CI builds fail