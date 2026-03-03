#!/bin/bash

# Skill Initialization Script
# This script provides quick access to the configured skills

case "$1" in
    "compile-check")
        echo "Running compiler error check..."
        npm run test:types
        ;;
    "smoke-test")
        echo "Running smoke tests..."
        npm test
        ;;
    "ci-status")
        echo "Checking CI status..."
        echo "Please check GitHub Actions at: https://github.com/Jmx097/lien-automation-v2/actions"
        ;;
    "new-branch")
        if [ -z "$2" ]; then
            echo "Usage: ./skills.sh new-branch <branch-name>"
            exit 1
        fi
        echo "Creating new branch: $2"
        git checkout -b "$2"
        ;;
    "help")
        echo "Available commands:"
        echo "  compile-check    - Run TypeScript type checking"
        echo "  smoke-test       - Run all tests"
        echo "  ci-status        - Check CI status"
        echo "  new-branch <name> - Create a new branch"
        echo "  help             - Show this help message"
        ;;
    *)
        echo "Lien Automation Skills Helper"
        echo "Usage: ./skills.sh <command>"
        echo "Run './skills.sh help' for available commands"
        ;;
esac