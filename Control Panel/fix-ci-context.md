# Fix CI Skill Context

## Project Information
- **Name**: Lien Automation v2
- **CI Platform**: GitHub Actions
- **Configuration**: .github/workflows/
- **Status**: Check GitHub Actions for current status

## Common CI Failure Points
1. **Dependency Installation**: npm install failures
2. **Type Checking**: TypeScript compilation errors
3. **Test Failures**: Unit or integration test failures
4. **Linting Issues**: Code style violations
5. **Build Issues**: Problems with the build process

## CI Debugging Process
1. Check the GitHub Actions tab for detailed logs
2. Identify the specific job and step that failed
3. Look for error messages or stack traces
4. Reproduce the issue locally if possible
5. Apply minimal fixes to resolve the issue

## Environment Differences
- CI environment may have different versions of tools
- Network restrictions might affect scraping tests
- Environment variables might be different
- Resource limitations (memory, CPU) might affect performance

## Fix Validation
1. Run the same commands locally that CI runs
2. Ensure environment matches CI as closely as possible
3. Test the fix thoroughly before pushing
4. Monitor CI status after pushing the fix

## Common Solutions
- Update dependencies in package.json
- Fix TypeScript type errors
- Adjust test expectations
- Update linting configuration
- Add missing environment variables