# Check Compiler Errors Skill Context

## Project Information
- **Name**: Lien Automation v2
- **Language**: TypeScript
- **Framework**: Node.js with Express.js
- **Build Tool**: npm

## Compile Commands
```bash
# Run TypeScript type checking
npm run test:types

# Build the project
npm run build
```

## Error Reporting Format
When reporting errors, please:
1. Group errors by file
2. Include the error message and line number
3. Suggest potential fixes when possible
4. Prioritize errors that prevent compilation

## Common Error Patterns
- Type mismatches in TypeScript files
- Missing dependencies
- Incorrect import statements
- Syntax errors

## Fix Strategy
1. Start with the first error in the list
2. Fix one error at a time
3. Re-run the compile command after each fix
4. Continue until all errors are resolved