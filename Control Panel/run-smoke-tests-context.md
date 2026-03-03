# Run Smoke Tests Skill Context

## Project Information
- **Name**: Lien Automation v2
- **Testing Framework**: Custom test scripts with TypeScript
- **Test Runner**: npm scripts

## Test Commands
```bash
# Run TypeScript type checking
npm run test:types

# Run selector smoke test
npm run test:selector-smoke

# Run full smoke test
npm run test:smoke

# Run all tests
npm test

# Run CA SOS range test (integration test)
npm run test:ca-sos-range
```

## Test Organization
- **Unit Tests**: Type checking and selector tests
- **Integration Tests**: CA SOS range tests that actually scrape data
- **Smoke Tests**: Health checks and basic functionality

## Debugging Information
When tests fail:
1. Check the output for specific error messages
2. Look for network connectivity issues
3. Verify environment variables are set correctly
4. Check if Bright Data Scraping Browser is accessible
5. Ensure Google Sheets credentials are valid

## Common Failure Points
- Network connectivity to Bright Data
- Invalid or expired credentials
- Rate limiting from the CA Secretary of State website
- Google Sheets API quotas

## Fix Strategy
1. Identify the specific test that is failing
2. Check logs for detailed error information
3. Apply minimal fixes to resolve the issue
4. Re-run the specific test to verify the fix
5. Run the full test suite to ensure no regressions