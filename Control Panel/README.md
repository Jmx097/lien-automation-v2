# Control Panel

This directory contains the necessary context and configuration files for OpenClaw skills that can assist with the development and maintenance of the lien automation project.

## Skills Context

The following skills have been configured with context files to help automate various development tasks:

1. **check-compiler-errors** - Run compile and type-check commands and report failures
2. **run-smoke-tests** - Run Playwright smoke tests, debug failures, and verify fixes
3. **new-branch-and-pr** - Create a fresh branch, complete work, and open a pull request
4. **fix-ci** - Find failing CI jobs, inspect logs, and apply focused fixes

Each skill has its own context file that provides the specific information needed for that skill to function properly with this project.