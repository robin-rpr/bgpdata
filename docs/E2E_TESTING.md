# End-to-End Testing

It is essential to maintain a high standard of code quality and consistency when adding new tests.
Follow the guidelines and best practices mentioned in this comment to contribute high-quality tests.

## Guidelines:

1.  Use descriptive variable names and keep the code DRY (Don't Repeat Yourself).
2.  Each utility function is documented explaining its purpose and usage. Do not remove these comments.
3.  Group tests logically using `test.describe`.
4.  Use `test.beforeEach` and `test.afterEach` for common setup and cleanup logic.
5.  When adding new utility functions, document them extensively, explaining the parameters and return values.
6.  Add appropriate timeout values to wait functions to prevent tests from hanging indefinitely in case of failures.
7.  Always verify no JS errors in console and the proper loading of widget functionality in each test.

## How to Extend:

1.  For testing new widgets, copy this template and replace the `widget` constant with the new widget name.
2.  Adjust the `testSet` constant to point to the new set of valid and invalid configurations.
3.  Customize the `verifyWidgetFunctionality` utility function to add widget-specific functionality tests.
4.  Add new utility functions as needed, following the guidelines mentioned above.

## Additional Resources:

- Playwright Documentation: https://playwright.dev/
- Playwright Test Runner: https://playwright.dev/docs/test-runners
- Playwright API: https://playwright.dev/docs/api/class-playwright
- JavaScript Testing Best Practices: https://github.com/goldbergyoni/javascript-testing-best-practices
