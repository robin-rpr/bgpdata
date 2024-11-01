/*** 
BGPDATA - BGP Data Collection and Analytics Service

This software is part of the BGPDATA project, which is designed to collect, process, and analyze BGP data from various sources.
It helps researchers and network operators get insights into their network by providing a scalable and reliable way to analyze and inspect historical and live BGP data from Route Collectors around the world.

Author: Robin Röper

© 2024 BGPDATA. All rights reserved.

Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:
1. Redistributions of source code must retain the above copyright notice, this list of conditions, and the following disclaimer.
2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions, and the following disclaimer in the documentation and/or other materials provided with the distribution.
3. Neither the name of BGPDATA nor the names of its contributors may be used to endorse or promote products derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
***/
import { expect } from "@playwright/test";
import { getPort } from "../main.mjs";

export async function initializeModule(
  page,
  module,
  configuration,
  settings = {},
) {
  await page.goto(`http://localhost:${getPort()}`);

  await page.evaluate(() => {
    const originalConsoleError = console.error;
    window.E2E_JS_ERRORS = [];

    console.error = (...args) => {
      window.E2E_JS_ERRORS.push(args);
      originalConsoleError(...args);
    };
  });

  await page.evaluate(
    ({ module, configuration, settings }) => {
      ripestat.create(
        module,
        configuration,
        "module-container", // Element to render the module at
        settings,
      );
    },
    { module, configuration, settings },
  );
}

export async function verifyJSNoErrors(page) {
  const jsErrors = await page.evaluate(() => window.E2E_JS_ERRORS.length);
  expect(jsErrors).toBe(0);
}

export async function verifyModuleLoad(page) {
  const moduleElement = await page.$("#module-container");
  expect(moduleElement).not.toBeNull();
}

export async function verifyModuleResult(page) {
  await page.waitForSelector("#module-container div.stat-module--loaded", {
    timeout: 5000,
  });
}

export async function verifyModuleError(page) {
  await page.waitForSelector("#module-container ul > li.error", {
    timeout: 5000,
  });
}

export async function verifyModuleFunctionality(page) {
  // Add additional checks for overall module functionality here

  // Example check for a shared element across modules
  await page.waitForSelector("#module-container", { timeout: 5000 });
}
