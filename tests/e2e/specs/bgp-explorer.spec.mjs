/*** 
BGPDATA - BGP Data Collection and Analytics Service

This software is part of the BGPDATA project, which is designed to collect, process, and analyze BGP data from various sources.
It helps researchers and network operators get insights into their network by providing a scalable and reliable way to analyze and inspect historical and live BGP data from RIPE NCC RIS.

Author: Robin Röper

© 2024 BGPDATA. All rights reserved.

Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:
1. Redistributions of source code must retain the above copyright notice, this list of conditions, and the following disclaimer.
2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions, and the following disclaimer in the documentation and/or other materials provided with the distribution.
3. Neither the name of BGPDATA nor the names of its contributors may be used to endorse or promote products derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
***/
import { test, expect } from "@playwright/test";
import { IPv4, IPv6, ASNs } from "../../data/resources.mjs";
import {
  initializeModule,
  verifyJSNoErrors,
  verifyModuleResult,
  verifyModuleError,
  verifyModuleFunctionality,
} from "../utils/test-module.mjs";

// Define module and resource configurations
const module = "bgp-explorer";
const testSet = [IPv4, IPv6, ASNs];

// This block contains tests for valid configurations.
// Loop through each valid resource configuration and perform tests to verify successful module results.
test.describe("Valid Configuration", () => {
  for (const resources of testSet) {
    resources.correct.forEach((value) => {
      test(`${value}`, async ({ page }) => {
        await initializeModule(page, module, { resource: value });
        await verifyModuleFunctionality(page);
        await verifyModuleResult(page);

        /* Define your custom code below to verify the Module's functionality on correct queries...
                    Leaving out this section is not acceptable. */

        // ...

        /* Check if any JavaScript Errors occurred */
        await verifyJSNoErrors(page);
      });
    });
  }
});

// This block contains tests for invalid configurations.
// Loop through each invalid resource configuration and perform tests to verify error handling in the module.
test.describe("Invalid Configuration", () => {
  for (const resources of testSet) {
    resources.incorrect.forEach((value) => {
      test(`${value}`, async ({ page }) => {
        await initializeModule(page, module, { resource: value });
        await verifyModuleFunctionality(page);
        await verifyModuleError(page);

        /* Define your custom code below to verify the Module's functionality on invlaid queries...
                    Leaving out this section is not acceptable. */

        // ...

        /* Check if any JavaScript Errors occurred */
        await verifyJSNoErrors(page);
      });
    });
  }
});
