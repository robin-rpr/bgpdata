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
import { IPAddr } from "../../../../static/scripts/utils/ip-address.js";
import { IPv4, IPv6 } from "../../../data/resources.mjs";

describe("IPAddr class", () => {
  describe("version", () => {
    describe("should return 4 for IPv4 addresses", () => {
      for (const address of IPv4.correct) {
        it(`${address}`, () => {
          const ip = new IPAddr(address);
          expect(ip.version).toBe(4);
        });
      }
    });

    describe("should return 6 for IPv6 addresses", () => {
      for (const address of IPv6.correct) {
        it(`${address}`, () => {
          const ip = new IPAddr(address);
          expect(ip.version).toBe(6);
        });
      }
    });
  });

  describe("message", () => {
    // No test cases nessesary and or possible
  });

  describe("cidr", () => {
    describe("should return CIDR for IPv4 addresses", () => {
      for (const address of IPv4.correct) {
        it(`${address}`, () => {
          const ip = new IPAddr(address);

          if (address.includes("/")) {
            // Extract the CIDR
            const cidr = parseInt(address.split("/")[1]);

            expect(ip.isValid()).toBe(true);
            expect(ip.cidr).toBe(cidr);
          } else {
            // No CIDR defined
            expect(ip.isValid()).toBe(true);
            expect(ip.cidr).toBe(0);
          }
        });
      }
    });

    describe("should return CIDR for IPv6 addresses", () => {
      for (const address of IPv6.correct) {
        it(`${address}`, () => {
          const ip = new IPAddr(address);

          if (address.includes("/")) {
            // Extract the CIDR
            const cidr = parseInt(address.split("/")[1]);

            expect(ip.isValid()).toBe(true);
            expect(ip.cidr).toBe(cidr);
          } else {
            // No CIDR defined
            expect(ip.isValid()).toBe(true);
            expect(ip.cidr).toBe(0);
          }
        });
      }
    });
  });

  describe("address", () => {
    /**
     * Expands a compressed IPv6 address to its full representation.
     * @param {string} ip - Compressed IPv6 address.
     * @returns {string} - Expanded IPv6 address.
     * @see {@link https://datatracker.ietf.org/doc/html/rfc4291#section-2.2|RFC 4291 Section 2.2}
     */
    function expandIPv6(ip) {
      let hextets = ip.split(":");
      if (ip.includes("::")) {
        let idx = hextets.indexOf("");
        let replacement = Array(9 - hextets.length).fill("0000");
        hextets.splice(idx, 1, ...replacement);
      }
      return hextets.map((h) => h.padStart(4, "0")).join(":");
    }

    describe("should return address for IPv4 addresses", () => {
      for (const address of IPv4.correct) {
        it(`${address}`, () => {
          const ip = new IPAddr(address);

          if (address.includes("/")) {
            expect(ip.address).toBe(address.split("/")[0]);
          } else {
            // No CIDR defined
            expect(ip.address).toBe(address);
          }
        });
      }
    });

    describe("should return address for IPv6 addresses", () => {
      for (const address of IPv6.correct) {
        it(`${address}`, () => {
          const ip = new IPAddr(address);

          if (address.includes("/")) {
            expect(ip.address).toBe(expandIPv6(address.split("/")[0]));
          } else {
            // No CIDR defined
            expect(ip.address).toBe(expandIPv6(address));
          }
        });
      }
    });
  });

  describe("compressed", () => {
    /**
     * Compresses a given IPv6 address string by removing leading zeros in each block
     * and replacing consecutive blocks of zeros with "::" as per the IPv6 addressing
     * standard defined in RFC 4291 (https://datatracker.ietf.org/doc/html/rfc4291).
     *
     * @function
     * @param {string} ip - The IPv6 address string to be compressed.
     * @returns {string} - The compressed IPv6 address string.
     * @throws Will throw an error if the input is not a valid IPv6 address string.
     * @example
     * // returns '2001:db8:0:42::8a2e:370:7334'
     * compressIPv6("2001:0db8:0000:0042:0000:8a2e:0370:7334");
     *
     * @see {@link https://datatracker.ietf.org/doc/html/rfc4291|RFC 4291}
     */
    function compressIPv6(ip) {
      //First remove the leading 0s of the octets. If it's '0000', replace with '0'
      let output = ip
        .split(":")
        .map((terms) => terms.replace(/\b0+/g, "") || "0")
        .join(":");

      //Then search for all occurrences of continuous '0' octets
      let zeros = [...output.matchAll(/\b:?(?:0+:?){2,}/g)];

      //If there are occurences, see which is the longest one and replace it with '::'
      if (zeros.length > 0) {
        let max = "";
        zeros.forEach((item) => {
          if (
            item[0].replaceAll(":", "").length > max.replaceAll(":", "").length
          ) {
            max = item[0];
          }
        });
        output = output.replace(max, "::");
      }
      return output;
    }

    describe("should return untouched IPv4 addresses", () => {
      for (const address of IPv4.correct) {
        it(`${address}`, () => {
          const ip = new IPAddr(address);

          if (address.includes("/")) {
            expect(ip.compressed).toBe(address.split("/")[0]);

            const compressed = new IPAddr(ip.compressed);
            expect(compressed.isValid()).toBe(true);
          } else {
            // No CIDR defined
            expect(ip.compressed).toBe(address);

            const compressed = new IPAddr(ip.compressed);
            expect(compressed.isValid()).toBe(true);
          }
        });
      }
    });

    describe("should return compressed IPv6 addresses", () => {
      for (const address of IPv6.correct) {
        it(`${address}`, () => {
          const ip = new IPAddr(address);

          if (address.includes("/")) {
            // Normalize the IPv6 address beforehand.
            const normalized = new IPAddr(address.split("/")[0]).address;

            expect(ip.compressed).toBe(compressIPv6(normalized));

            const compressed = new IPAddr(ip.compressed);
            expect(compressed.isValid()).toBe(true);
          } else {
            // No CIDR defined

            // Normalize the IPv6 address beforehand.
            const normalized = new IPAddr(address.split("/")[0]).address;

            expect(ip.compressed).toBe(compressIPv6(normalized));

            const compressed = new IPAddr(ip.compressed);
            expect(compressed.isValid()).toBe(true);
          }
        });
      }
    });
  });

  describe("wildcard", () => {
    /**
     * Calculate wildcard mask for IPv4 and IPv6 addresses.
     *
     * @param {string} ip - The IP address (IPv4 or IPv6).
     * @param {number} prefixLength - The prefix length (CIDR notation).
     * @returns {string} - The wildcard mask.
     *
     * @throws Will throw an error if the IP address or prefix length is invalid.
     *
     * @see RFC791 {@link https://datatracker.ietf.org/doc/html/rfc791}
     * @see RFC4291 {@link https://datatracker.ietf.org/doc/html/rfc4291}
     */
    function calculateWildcard(ip, prefixLength) {
      if (
        typeof ip !== "string" ||
        typeof prefixLength !== "number" ||
        prefixLength < 0
      ) {
        throw new Error("Invalid input");
      }

      // Check if the IP is IPv4 or IPv6
      const isIPv4 = ip.includes(".");
      const isIPv6 = ip.includes(":");

      if (isIPv4) {
        if (prefixLength > 32) {
          throw new Error("Invalid prefix length for IPv4");
        }

        const chunks = [];

        let bitmask =
          "1".repeat(parseInt(prefixLength)) + "0".repeat(32 - prefixLength);
        bitmask = [...bitmask].map((bit) => (bit === "1" ? "0" : "1")).join("");

        while (bitmask) {
          chunks.push(parseInt(bitmask.slice(0, 8), 2)); // Convert binary to decimal
          bitmask = bitmask.slice(8);
        }

        while (chunks.length < 4) {
          chunks.push(0); // Pad with zeros if necessary
        }

        return chunks.join(".");
      } else if (isIPv6) {
        if (prefixLength > 128) {
          throw new Error("Invalid prefix length for IPv6 address");
        }

        const chunks = [];

        let bitmask =
          "1".repeat(parseInt(prefixLength)) + "0".repeat(128 - prefixLength);
        bitmask = [...bitmask].map((bit) => (bit === "1" ? "0" : "1")).join("");

        while (bitmask) {
          chunks.push(parseInt(bitmask.slice(0, 16), 2).toString(16)); // Convert binary to hexadecimal
          bitmask = bitmask.slice(16);
        }

        while (chunks.length < 8) {
          chunks.push("0"); // Pad with zeros if necessary
        }

        return chunks.join(":");
      } else {
        throw new Error("Invalid IP address");
      }
    }

    describe("should return IPv4 wildcard addresses", () => {
      for (const address of IPv4.correct) {
        it(`${address}`, () => {
          const ip = new IPAddr(address);

          if (address.includes("/")) {
            const cidr = parseInt(address.split("/")[1]);

            expect(ip.wildcard.address).toBe(
              new IPAddr(calculateWildcard(address.split("/")[0], cidr))
                .address,
            );
          } else {
            // No CIDR defined
            expect(ip.wildcard.address).toBe(
              new IPAddr(calculateWildcard(address, 0)).address,
            );
          }
        });
      }
    });

    describe("should return IPv6 wildcard addresses", () => {
      for (const address of IPv6.correct) {
        it(`${address}`, () => {
          const ip = new IPAddr(address);

          if (address.includes("/")) {
            const cidr = parseInt(address.split("/")[1]);

            expect(ip.wildcard.address).toBe(
              new IPAddr(calculateWildcard(address.split("/")[0], cidr))
                .address,
            );
          } else {
            // No CIDR defined
            expect(ip.wildcard.address).toBe(
              new IPAddr(calculateWildcard(address, 0)).address,
            );
          }
        });
      }
    });
  });

  describe("netmask", () => {
    /**
     * Calculate netmask for IPv4 and IPv6 addresses.
     *
     * @param {string} ip - The IP address in string format.
     * @param {number} prefixLength - The prefix length (CIDR notation).
     * @returns {string} - The calculated netmask.
     * @throws Will throw an error if the IP address or prefix length is invalid.
     *
     * @see {@link https://datatracker.ietf.org/doc/html/rfc791|RFC 791} for IPv4 address specification.
     * @see {@link https://datatracker.ietf.org/doc/html/rfc4291|RFC 4291} for IPv6 address specification.
     */
    function calculateNetmask(ip, prefixLength) {
      if (
        typeof ip !== "string" ||
        typeof prefixLength !== "number" ||
        prefixLength < 0
      ) {
        throw new Error("Invalid IP address or prefix length");
      }

      // Check if the IP address is IPv4 or IPv6
      const isIPv4 = ip.includes(".");
      const isIPv6 = ip.includes(":");

      if (isIPv4) {
        if (prefixLength > 32) {
          throw new Error("Invalid prefix length for IPv4 address");
        }

        const chunks = [];
        let bitmask =
          "1".repeat(parseInt(prefixLength)) + "0".repeat(32 - prefixLength);

        while (bitmask) {
          chunks.push(parseInt(bitmask.slice(0, 8), 2)); // Convert binary to decimal
          bitmask = bitmask.slice(8);
        }

        while (chunks.length < 4) {
          chunks.push(0); // Pad with zeros if necessary
        }

        return chunks.join(".");
      } else if (isIPv6) {
        if (prefixLength > 128) {
          throw new Error("Invalid prefix length for IPv6 address");
        }

        const chunks = [];
        let bitmask =
          "1".repeat(parseInt(prefixLength)) + "0".repeat(128 - prefixLength);

        while (bitmask) {
          chunks.push(parseInt(bitmask.slice(0, 16), 2).toString(16)); // Convert binary to hexadecimal
          bitmask = bitmask.slice(16);
        }

        while (chunks.length < 8) {
          chunks.push("0"); // Pad with zeros if necessary
        }

        return chunks.join(":");
      } else {
        throw new Error("Invalid IP address format");
      }
    }

    describe("should return IPv4 netmask addresses", () => {
      for (const address of IPv4.correct) {
        it(`${address}`, () => {
          const ip = new IPAddr(address);

          if (address.includes("/")) {
            const cidr = parseInt(address.split("/")[1]);

            expect(ip.netmask.address).toBe(
              new IPAddr(calculateNetmask(address.split("/")[0], cidr)).address,
            );
          } else {
            // No CIDR defined
            expect(ip.netmask.address).toBe(
              new IPAddr(calculateNetmask(address, 0)).address,
            );
          }
        });
      }
    });

    describe("should return IPv6 netmask addresses", () => {
      for (const address of IPv6.correct) {
        it(`${address}`, () => {
          const ip = new IPAddr(address);

          if (address.includes("/")) {
            const cidr = parseInt(address.split("/")[1]);

            expect(ip.netmask.address).toBe(
              new IPAddr(calculateNetmask(address.split("/")[0], cidr)).address,
            );
          } else {
            // No CIDR defined
            expect(ip.netmask.address).toBe(
              new IPAddr(calculateNetmask(address, 0)).address,
            );
          }
        });
      }
    });
  });

  describe("network", () => {
    /**
     * Calculate network address from IP address and subnet mask/prefix length.
     *
     * @param {string} ip - The IP address in dotted-decimal (IPv4) or hexadecimal (IPv6) format.
     * @param {number} prefixLength - The prefix length (CIDR notation).
     * @returns {string} - The network address in appropriate format.
     *
     * @see RFC 791 {@link https://datatracker.ietf.org/doc/html/rfc791}
     * @see RFC 4291 {@link https://datatracker.ietf.org/doc/html/rfc4291}
     */
    function calculateNetwork(ip, prefixLength) {
      if (
        typeof ip !== "string" ||
        typeof prefixLength !== "number" ||
        prefixLength < 0
      ) {
        throw new Error("Invalid IP address or prefix length");
      }

      /**
       * Expands a compressed IPv6 address to its full representation.
       * @param {string} ip - Compressed IPv6 address.
       * @returns {string} - Expanded IPv6 address.
       * @see {@link https://datatracker.ietf.org/doc/html/rfc4291#section-2.2|RFC 4291 Section 2.2}
       */
      function expandIPv6(ip) {
        let hextets = ip.split(":");
        if (ip.includes("::")) {
          let idx = hextets.indexOf("");
          let replacement = Array(9 - hextets.length).fill("0000");
          hextets.splice(idx, 1, ...replacement);
        }
        return hextets.map((h) => h.padStart(4, "0")).join(":");
      }

      // Check if the IP address is IPv4 or IPv6
      const isIPv4 = ip.includes(".");
      const isIPv6 = ip.includes(":");

      if (isIPv4) {
        if (prefixLength > 32) {
          throw new Error("Invalid prefix length for IPv4 address");
        }

        const mask = [];
        let bitmask =
          "1".repeat(parseInt(prefixLength)) + "0".repeat(32 - prefixLength);

        while (bitmask) {
          mask.push(parseInt(bitmask.slice(0, 8), 2)); // Convert binary to decimal
          bitmask = bitmask.slice(8);
        }

        while (mask.length < 4) {
          mask.push(0); // Pad with zeros if necessary
        }

        const chunks = [...ip.split(".")].map((quad, i) => quad & mask[i]);

        return chunks.join(".");
      } else if (isIPv6) {
        if (prefixLength > 128) {
          throw new Error("Invalid prefix length for IPv6 address");
        }

        const mask = [];
        let bitmask =
          "1".repeat(parseInt(prefixLength)) + "0".repeat(128 - prefixLength);

        while (bitmask) {
          mask.push(parseInt(bitmask.slice(0, 16), 2).toString(16)); // Convert binary to hexadecimal
          bitmask = bitmask.slice(16);
        }

        while (mask.length < 8) {
          mask.push("0"); // Pad with zeros if necessary
        }

        const chunks = expandIPv6(ip)
          .split(":")
          .map((segment, i) =>
            (parseInt(segment, 16) & parseInt(mask[i], 16))
              .toString(16)
              .toUpperCase(),
          )
          .join(":");

        return expandIPv6(chunks);
      } else {
        throw new Error("Invalid IP address format");
      }
    }
  });

  describe("neighbors", () => {
    describe("should return IPv4 neighbors addresses", () => {
      for (const address of IPv4.correct) {
        it(`${address}`, () => {
          const ip = new IPAddr(address);
          const cidr = parseInt(address.split("/")[1]);

          if (address.includes("/") && cidr) {
            if (cidr >= 32) {
              expect(ip.neighbors.up).not.toBe(null);
              expect(new IPAddr(ip.neighbors.up).isValid()).toBe(true);

              expect(ip.neighbors.left).not.toBe(null);
              expect(new IPAddr(ip.neighbors.left).isValid()).toBe(true);

              expect(ip.neighbors.right).not.toBe(null);
              expect(new IPAddr(ip.neighbors.right).isValid()).toBe(true);

              expect(ip.neighbors.downleft).toBe(null);
              expect(ip.neighbors.downright).toBe(null);
            } else {
              expect(ip.neighbors.up).not.toBe(null);
              expect(new IPAddr(ip.neighbors.up).isValid()).toBe(true);

              expect(ip.neighbors.left).not.toBe(null);
              expect(new IPAddr(ip.neighbors.left).isValid()).toBe(true);

              expect(ip.neighbors.right).not.toBe(null);
              expect(new IPAddr(ip.neighbors.right).isValid()).toBe(true);

              expect(ip.neighbors.downleft).not.toBe(null);
              expect(new IPAddr(ip.neighbors.downleft).isValid()).toBe(true);

              expect(ip.neighbors.downright).not.toBe(null);
              expect(new IPAddr(ip.neighbors.downright).isValid()).toBe(true);
            }
          }
        });
      }
    });

    describe("should return IPv6 neighbors addresses", () => {
      for (const address of IPv6.correct) {
        it(`${address}`, () => {
          const ip = new IPAddr(address);
          const cidr = parseInt(address.split("/")[1]);

          if (address.includes("/") && cidr) {
            if (cidr >= 128) {
              expect(ip.neighbors.up).not.toBe(null);
              expect(new IPAddr(ip.neighbors.up).isValid()).toBe(true);

              expect(ip.neighbors.left).not.toBe(null);
              expect(new IPAddr(ip.neighbors.left).isValid()).toBe(true);

              expect(ip.neighbors.right).not.toBe(null);
              expect(new IPAddr(ip.neighbors.right).isValid()).toBe(true);

              expect(ip.neighbors.downleft).toBe(null);
              expect(ip.neighbors.downright).toBe(null);
            } else {
              expect(ip.neighbors.up).not.toBe(null);
              expect(new IPAddr(ip.neighbors.up).isValid()).toBe(true);

              expect(ip.neighbors.left).not.toBe(null);
              expect(new IPAddr(ip.neighbors.left).isValid()).toBe(true);

              expect(ip.neighbors.right).not.toBe(null);
              expect(new IPAddr(ip.neighbors.right).isValid()).toBe(true);

              expect(ip.neighbors.downleft).not.toBe(null);
              expect(new IPAddr(ip.neighbors.downleft).isValid()).toBe(true);

              expect(ip.neighbors.downright).not.toBe(null);
              expect(new IPAddr(ip.neighbors.downright).isValid()).toBe(true);
            }
          }
        });
      }
    });
  });

  describe("bitmask", () => {
    /**
     * Calculate bitmask for IPv4 and IPv6 addresses.
     *
     * @param {string} ip - The IP address (IPv4 or IPv6).
     * @param {number} prefixLength - The prefix length (CIDR notation).
     * @returns {string} - The wildcard mask.
     *
     * @throws Will throw an error if the IP address or prefix length is invalid.
     */
    function calculateBitmask(ip, prefixLength) {
      if (
        typeof ip !== "string" ||
        typeof prefixLength !== "number" ||
        prefixLength < 0
      ) {
        throw new Error("Invalid IP address or prefix length");
      }

      // Check if the IP address is IPv4 or IPv6
      const isIPv4 = ip.includes(".");
      const isIPv6 = ip.includes(":");

      if (isIPv4) {
        return (
          "1".repeat(parseInt(prefixLength)) + "0".repeat(32 - prefixLength)
        );
      } else if (isIPv6) {
        return (
          "1".repeat(parseInt(prefixLength)) + "0".repeat(128 - prefixLength)
        );
      }
    }

    describe("should return IPv4 bitmask", () => {
      for (const address of IPv4.correct) {
        it(`${address}`, () => {
          const ip = new IPAddr(address);

          if (address.includes("/")) {
            const cidr = parseInt(address.split("/")[1]);

            expect(ip.bitmask).toBe(
              calculateBitmask(address.split("/")[0], cidr),
            );
          } else {
            // No CIDR defined
            expect(ip.bitmask).toBe(calculateBitmask(address, 0));
          }
        });
      }
    });

    describe("should return IPv6 bitmask", () => {
      for (const address of IPv6.correct) {
        it(`${address}`, () => {
          const ip = new IPAddr(address);

          if (address.includes("/")) {
            const cidr = parseInt(address.split("/")[1]);

            expect(ip.bitmask).toBe(
              calculateBitmask(address.split("/")[0], cidr),
            );
          } else {
            // No CIDR defined
            expect(ip.bitmask).toBe(calculateBitmask(address, 0));
          }
        });
      }
    });
  });

  describe("hostCount", () => {
    describe("should return IPv4 host count", () => {
      for (const address of IPv4.correct) {
        it(`${address}`, () => {
          const ip = new IPAddr(address);

          if (address.includes("/")) {
            const cidr = parseInt(address.split("/")[1]);

            expect(ip.hostCount).toBe(
              BigInt(Math.pow(2, parseInt(32 - cidr)).toString()),
            );
          } else {
            // No CIDR defined
            expect(ip.hostCount).toBe(BigInt(Math.pow(2, 32).toString()));
          }
        });
      }
    });

    describe("should return IPv6 host count", () => {
      for (const address of IPv6.correct) {
        it(`${address}`, () => {
          const ip = new IPAddr(address);

          if (address.includes("/")) {
            const cidr = parseInt(address.split("/")[1]);

            expect(ip.hostCount).toBe(
              BigInt(IPAddr.v6cidrs[parseInt(128 - cidr)]),
            );
          } else {
            // No CIDR defined
            expect(ip.hostCount).toBe(BigInt(IPAddr.v6cidrs[128]));
          }
        });
      }
    });
  });

  describe("maxHost", () => {
    describe("should return IPv4 max host addresses", () => {
      for (const address of IPv4.correct) {
        it(`${address}`, () => {
          const ip = new IPAddr(address);
          expect(ip.maxHost.isValid()).toBe(true);
        });
      }
    });

    describe("should return IPv6 max host addresses", () => {
      for (const address of IPv6.correct) {
        it(`${address}`, () => {
          const ip = new IPAddr(address);
          expect(ip.maxHost.isValid()).toBe(true);
        });
      }
    });
  });

  describe("minHost", () => {
    describe("should return IPv4 min host addresses", () => {
      for (const address of IPv4.correct) {
        it(`${address}`, () => {
          const ip = new IPAddr(address);
          expect(ip.minHost.isValid()).toBe(true);
        });
      }
    });

    describe("should return IPv6 min host addresses", () => {
      for (const address of IPv6.correct) {
        it(`${address}`, () => {
          const ip = new IPAddr(address);
          expect(ip.minHost.isValid()).toBe(true);
        });
      }
    });
  });

  describe("broadcast", () => {
    describe("should return broadcast addresses on IPv4 addresses", () => {
      for (const address of IPv4.correct) {
        it(`${address}`, () => {
          const ip = new IPAddr(address);
          expect(ip.broadcast.isValid()).toBe(true);
        });
      }
    });

    describe("should return null on IPv6 addresses", () => {
      for (const address of IPv6.correct) {
        it(`${address}`, () => {
          const ip = new IPAddr(address);
          expect(ip.broadcast).toBe(null);
        });
      }
    });
  });

  describe("expanded", () => {
    describe("should return expanded IPv4 addresses", () => {
      for (const address of IPv4.correct) {
        it(`${address}`, () => {
          const ip = new IPAddr(address);
          expect(new IPAddr(ip.expanded).isValid()).toBe(true);

          // TODO: Add more Tests to check if new IPAddr(...).expanded using IPv4 works as expected.
        });
      }
    });

    describe("should return expanded IPv6 addresses", () => {
      for (const address of IPv6.correct) {
        it(`${address}`, () => {
          const ip = new IPAddr(address);
          expect(new IPAddr(ip.expanded).isValid()).toBe(true);

          // TODO: Add more Tests to check if new IPAddr(...).expanded using IPv6 works as expected.
        });
      }
    });
  });

  describe("mappedIPv6", () => {
    /**
     * Converts an IPv4 address to an IPv4-mapped IPv6 address.
     *
     * This function takes an IPv4 address and a prefix length as input and returns
     * an IPv4-mapped IPv6 address with an adjusted prefix length. The IPv4-mapped
     * IPv6 address is formed by appending the hexadecimal representation of the IPv4
     * address to the well-known prefix "::FFFF:". The prefix length of the resulting
     * IPv6 address is calculated by adding 96 to the input IPv4 prefix length.
     *
     * IPv4-mapped IPv6 addresses are described in RFC 4291, section 2.5.5.2. The
     * function follows the textual representation of IPv6 addresses as outlined in
     * RFC 5952.
     *
     * @example
     * // Returns '::FFFF:c0a8:0101/120'
     * mapToIPv6('192.168.1.1', 24);
     *
     * @param {string} ip - The IPv4 address in dotted-decimal notation.
     * @param {number} prefixLength - The prefix length of the IPv4 address.
     * @returns {string} The IPv4-mapped IPv6 address with adjusted prefix length.
     * @throws {TypeError} If `ip` is not a string or `prefixLength` is not a number.
     * @throws {Error} If `ip` is not a valid IPv4 address or `prefixLength` is not a valid prefix length.
     * @see {@link https://datatracker.ietf.org/doc/html/rfc4291#section-2.5.5.2|RFC 4291: IP Version 6 Addressing Architecture}
     * @see {@link https://datatracker.ietf.org/doc/html/rfc5952|RFC 5952: A Recommendation for IPv6 Address Text Representation}
     */
    function mapToIPv6(ip, prefixLength) {
      if (typeof ip !== "string" || !ip.match(/^\d{1,3}(\.\d{1,3}){3}$/)) {
        throw new TypeError(
          "ip must be a string representing a valid IPv4 address",
        );
      }
      if (
        typeof prefixLength !== "number" ||
        prefixLength < 0 ||
        prefixLength > 32
      ) {
        throw new TypeError("prefixLength must be a number between 0 and 32");
      }

      const chunks = ("::FFFF:" + ip).match(/^([0:]+:FFFF:)([\d.]+)$/i);
      const ipv6 = chunks[1];
      const ipv4 = chunks[2];
      const quads = [];
      const octets = ipv4.split(".");

      for (var i = 0; i < octets.length; i++) {
        var quad = parseInt(octets[i]).toString(16);
        if (quad.length === 1) quad = "0" + quad;
        quads.push(quad);
      }

      const mapped = ipv6 + quads[0] + quads[1] + ":" + quads[2] + quads[3];
      return `${mapped}/${prefixLength + 96}`;
    }

    describe("should return IPv4 addresses mapped to IPv6", () => {
      for (const address of IPv4.correct) {
        it(`${address}`, () => {
          const ip = new IPAddr(address);

          expect(ip.mappedIPv6.address).toBe(
            new IPAddr(mapToIPv6(ip.address, ip.cidr)).address,
          );
          expect(ip.mappedIPv6.cidr).toBe(
            new IPAddr(mapToIPv6(ip.address, ip.cidr)).cidr,
          );
          expect(ip.mappedIPv6.isValid()).toBe(true);
        });
      }
    });

    describe("should return untouched IPv6 addresses", () => {
      for (const address of IPv6.correct) {
        it(`${address}`, () => {
          const ip = new IPAddr(address);

          expect(ip.mappedIPv6.address).toBe(ip.address);
          expect(ip.mappedIPv6.cidr).toBe(ip.cidr);
          expect(ip.mappedIPv6.isValid()).toBe(true);
        });
      }
    });
  });

  describe("add(...)", () => {
    describe("should return added IPv4 addresses", () => {
      for (const address of IPv4.correct) {
        it(`${address}`, () => {
          const ip = new IPAddr(address);

          expect(ip.add(ip).subtract(ip).address).toBe(ip.address);
          expect(ip.add(ip).subtract(ip).cidr).toBe(ip.cidr);

          expect(ip.add(ip).isValid()).toBe(true);
        });
      }
    });

    describe("should return added IPv6 addresses", () => {
      for (const address of IPv6.correct) {
        it(`${address}`, () => {
          const ip = new IPAddr(address);

          expect(ip.add(ip).subtract(ip).address).toBe(ip.address);
          expect(ip.add(ip).subtract(ip).cidr).toBe(ip.cidr);

          expect(ip.add(ip).isValid()).toBe(true);
        });
      }
    });
  });

  describe("subtract(...)", () => {
    describe("should return subtracted IPv4 addresses", () => {
      for (const address of IPv4.correct) {
        it(`${address}`, () => {
          const ip = new IPAddr(address);

          expect(ip.subtract(ip).add(ip).address).toBe(ip.address);
          expect(ip.subtract(ip).add(ip).cidr).toBe(ip.cidr);

          expect(ip.subtract(ip).isValid()).toBe(true);
        });
      }
    });

    describe("should return subtracted IPv6 addresses", () => {
      for (const address of IPv6.correct) {
        it(`${address}`, () => {
          const ip = new IPAddr(address);

          expect(ip.subtract(ip).add(ip).address).toBe(ip.address);
          expect(ip.subtract(ip).add(ip).cidr).toBe(ip.cidr);

          expect(ip.subtract(ip).isValid()).toBe(true);
        });
      }
    });
  });

  describe("compare(...)", () => {
    describe("should return compairson of IPv4 addresses", () => {
      for (const address of IPv4.correct) {
        it(`${address}`, () => {
          const ip = new IPAddr(address);
          ip.cidr = 24; // Ensure that minHost !== maxHost by setting /24 CIDR.

          expect(ip.compare(ip)).toBe(0);
          expect(ip.minHost.compare(ip.maxHost)).toBe(-1);
          expect(ip.maxHost.compare(ip.minHost)).toBe(1);
        });
      }
    });

    describe("should return compairson of IPv6 addresses", () => {
      for (const address of IPv6.correct) {
        it(`${address}`, () => {
          const ip = new IPAddr(address);
          ip.cidr = 48; // Ensure that minHost !== maxHost by setting /48 CIDR.

          expect(ip.compare(ip)).toBe(0);
          expect(ip.minHost.compare(ip.maxHost)).toBe(-1);
          expect(ip.maxHost.compare(ip.minHost)).toBe(1);
        });
      }
    });
  });

  describe("overlap(...)", () => {
    describe("should return overlap of IPv4 addresses", () => {
      for (const address of IPv4.correct) {
        it(`${address}`, () => {
          const ip = new IPAddr(address);
          ip.cidr = 24; // Ensure that minHost !== maxHost by setting /24 CIDR.

          // 100%-overlap
          expect(ip.overlap(ip)).toStrictEqual([0, 100]);
          expect(ip.minHost.overlap(ip.maxHost)).toStrictEqual([0, 100]);
          expect(ip.maxHost.overlap(ip.minHost)).toStrictEqual([0, 100]);

          // TODO: Add tests to check for correct detection of 50%-overlapping and 0%-overlapping.
        });
      }
    });

    describe("should return overlap of IPv6 addresses", () => {
      for (const address of IPv6.correct) {
        it(`${address}`, () => {
          const ip = new IPAddr(address);
          ip.cidr = 48; // Ensure that minHost !== maxHost by setting /48 CIDR.

          // 100%-overlap
          expect(ip.overlap(ip)).toStrictEqual([0, 100]);
          expect(ip.minHost.overlap(ip.maxHost)).toStrictEqual([0, 100]);
          expect(ip.maxHost.overlap(ip.minHost)).toStrictEqual([0, 100]);

          // TODO: Add tests to check for correct detection of 50%-overlapping and 0%-overlapping.
        });
      }
    });
  });

  describe("fromOctets(...)", () => {
    describe("should return IPv4 addresses from IPv4 address octets", () => {
      for (const address of IPv4.correct) {
        it(`${address}`, () => {
          const ip = new IPAddr(address);

          const octets = ip.address.split(".").map((octet) => parseInt(octet));

          expect(IPAddr.fromOctets(octets).address).toBe(ip.address);
          expect(IPAddr.fromOctets(octets).isValid()).toBe(true);
        });
      }
    });
  });

  describe("fromSegments(...)", () => {
    describe("should return IPv6 addresses from IPv6 address segments", () => {
      for (const address of IPv6.correct) {
        it(`${address}`, () => {
          const ip = new IPAddr(address);

          const segments = ip.address
            .split(":")
            .map((segment) => parseInt(segment, 16));

          expect(IPAddr.fromSegments(segments).address).toBe(ip.address);
          expect(IPAddr.fromSegments(segments).isValid()).toBe(true);
        });
      }
    });
  });

  describe("fromRange(...)", () => {
    describe("should return IPv4 addresses from IPv4 address ranges", () => {
      for (const address of IPv4.correct) {
        it(`${address}`, () => {
          const ip = new IPAddr(address);
          const range = `${ip.network.address}-${ip.broadcast.address}`;

          if (ip.cidr === 32) {
            // Probe maximum IPv4 CIDR
            expect(() => IPAddr.fromRange(range)).toThrow();
          } else {
            expect(IPAddr.fromRange(range).address).toBe(ip.network.address);
            expect(IPAddr.fromRange(range).cidr).toBe(ip.cidr);
            expect(IPAddr.fromRange(range).isValid()).toBe(true);
          }
        });
      }
    });

    describe("should return IPv6 addresses from IPv6 address ranges", () => {
      for (const address of IPv6.correct) {
        it(`${address}`, () => {
          const ip = new IPAddr(address);
          const range = `${ip.network.address}-${ip.maxHost.address}`;

          if (ip.cidr === 128) {
            // Probe maximum IPv6 CIDR
            expect(() => IPAddr.fromRange(range)).toThrow();
          } else {
            expect(IPAddr.fromRange(range).address).toBe(ip.network.address);
            expect(IPAddr.fromRange(range).cidr).toBe(ip.cidr);
            expect(IPAddr.fromRange(range).isValid()).toBe(true);
          }
        });
      }
    });
  });

  describe("toString(...)", () => {
    describe("should return IPv4 addresses as prefixed strings", () => {
      for (const address of IPv4.correct) {
        it(`${address}`, () => {
          const ip = new IPAddr(address);

          if (address.includes("/")) {
            expect(ip.toString()).toBe(`${ip.address}/${ip.cidr}`);
            expect(ip.toString("cidr")).toBe(`${ip.address}/${ip.cidr}`);
          } else {
            // No CIDR defined
            expect(ip.toString()).toBe(`${ip.address}/0`);
            expect(ip.toString("cidr")).toBe(`${ip.address}/0`);
          }
        });
      }
    });

    describe("should return IPv6 addresses as prefixed strings", () => {
      for (const address of IPv6.correct) {
        it(`${address}`, () => {
          const ip = new IPAddr(address);

          if (address.includes("/")) {
            expect(ip.toString()).toBe(`${ip.address}/${ip.cidr}`);
            expect(ip.toString("cidr")).toBe(`${ip.address}/${ip.cidr}`);
          } else {
            // No CIDR defined
            expect(ip.toString()).toBe(`${ip.address}/0`);
            expect(ip.toString("cidr")).toBe(`${ip.address}/0`);
          }
        });
      }
    });

    describe("should return IPv4 addresses as range strings", () => {
      for (const address of IPv4.correct) {
        it(`${address}`, () => {
          const ip = new IPAddr(address);

          expect(ip.toString("range")).toBe(
            `${ip.address}-${ip.broadcast.address}`,
          );
        });
      }
    });

    describe("should return IPv6 addresses as range strings", () => {
      for (const address of IPv6.correct) {
        it(`${address}`, () => {
          const ip = new IPAddr(address);

          expect(ip.toString("range")).toBe(
            `${ip.address}-${ip.maxHost.address}`,
          );
        });
      }
    });
  });

  describe("toBinaryString(...)", () => {
    /**
     * Converts an IPv4 or IPv6 address to a binary string.
     *
     * For IPv4, the function follows the standard specified in RFC 791.
     * For IPv6, the function follows the standard specified in RFC 4291.
     *
     * @param {string} ip - The IPv4 or IPv6 address to be converted.
     * @returns {string} - The binary string representation of the IP address.
     * @throws {Error} - Throws an error if the input is not a valid IPv4 or IPv6 address.
     * @see {@link https://datatracker.ietf.org/doc/html/rfc791|RFC 791}
     * @see {@link https://datatracker.ietf.org/doc/html/rfc4291|RFC 4291}
     */
    function toBinaryString(ip) {
      if (typeof ip !== "string") {
        throw new Error("Input must be a string");
      }

      // Helper function to pad binary strings
      function padBin(bin, length) {
        return bin.padStart(length, "0");
      }

      // Validate and convert IPv4 address
      if (/^(\d{1,3}\.){3}\d{1,3}$/.test(ip)) {
        return ip
          .split(".")
          .map((octet) => {
            if (octet < 0 || octet > 255) {
              throw new Error("Invalid IPv4 address");
            }
            return padBin(parseInt(octet, 10).toString(2), 8);
          })
          .join("");
      }

      // Validate and convert IPv6 address
      if (/^([0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}$/.test(ip)) {
        return ip
          .split(":")
          .map((segment) => padBin(parseInt(segment, 16).toString(2), 16))
          .join("");
      }

      // Handle IPv6 address with double colon
      if (/^([0-9a-fA-F]{1,4}:){1,6}(:[0-9a-fA-F]{1,4}){1,6}$/.test(ip)) {
        const segments = ip.split(":");
        const fillSegments =
          8 - segments.filter((segment) => segment !== "").length;
        return segments
          .map((segment) =>
            segment === ""
              ? padBin("", 16).repeat(fillSegments)
              : padBin(parseInt(segment, 16).toString(2), 16),
          )
          .join("");
      }

      throw new Error("Invalid IP address");
    }

    describe("should return IPv4 addresses as binary strings", () => {
      for (const address of IPv4.correct) {
        it(`${address}`, () => {
          const ip = new IPAddr(address);
          expect(ip.toBinaryString()).toBe(toBinaryString(ip.address));
        });
      }
    });

    describe("should return IPv6 addresses as binary strings", () => {
      for (const address of IPv6.correct) {
        it(`${address}`, () => {
          const ip = new IPAddr(address);
          expect(ip.toBinaryString()).toBe(toBinaryString(ip.address));
        });
      }
    });
  });

  describe("toBinaryArray(...)", () => {
    /**
     * Converts an IPv4 or IPv6 address to an array of binary integers.
     *
     * For IPv4, the function follows the standard specified in RFC 791.
     * For IPv6, the function follows the standard specified in RFC 4291.
     *
     * @param {string} ip - The IPv4 or IPv6 address to be converted.
     * @returns {number[]} - The array of binary integers representing the IP address.
     * @throws {Error} - Throws an error if the input is not a valid IPv4 or IPv6 address.
     * @see {@link https://datatracker.ietf.org/doc/html/rfc791|RFC 791}
     * @see {@link https://datatracker.ietf.org/doc/html/rfc4291|RFC 4291}
     */
    function toBinaryArray(ip) {
      if (typeof ip !== "string") {
        throw new Error("Input must be a string");
      }

      // Helper function to pad binary strings
      function padBin(bin, length) {
        return bin.padStart(length, "0");
      }

      // Validate and convert IPv4 address
      if (/^(\d{1,3}\.){3}\d{1,3}$/.test(ip)) {
        return ip.split(".").map((octet) => {
          if (octet < 0 || octet > 255) {
            throw new Error("Invalid IPv4 address");
          }
          return parseInt(padBin(parseInt(octet, 10).toString(2), 8), 2);
        });
      }

      // Validate and convert IPv6 address
      if (/^([0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}$/.test(ip)) {
        return ip
          .split(":")
          .map((segment) =>
            parseInt(padBin(parseInt(segment, 16).toString(2), 16), 2),
          );
      }

      // Handle IPv6 address with double colon
      if (/^([0-9a-fA-F]{1,4}:){1,6}(:[0-9a-fA-F]{1,4}){1,6}$/.test(ip)) {
        const segments = ip.split(":");
        const fillSegments =
          8 - segments.filter((segment) => segment !== "").length;
        return segments
          .map((segment) =>
            segment === ""
              ? Array(fillSegments).fill(0)
              : [parseInt(padBin(parseInt(segment, 16).toString(2), 16), 2)],
          )
          .flat();
      }

      throw new Error("Invalid IP address");
    }

    describe("should return IPv4 addresses as binary array", () => {
      for (const address of IPv4.correct) {
        it(`${address}`, () => {
          const ip = new IPAddr(address);
          expect(ip.toBinaryArray()).toStrictEqual(toBinaryArray(ip.address));
        });
      }
    });

    describe("should return IPv6 addresses as binary array", () => {
      for (const address of IPv6.correct) {
        it(`${address}`, () => {
          const ip = new IPAddr(address);
          expect(ip.toBinaryArray()).toStrictEqual(toBinaryArray(ip.address));
        });
      }
    });
  });

  describe("getRelativePosition(...)", () => {
    describe("should return IPv4 addresses relative position in range", () => {
      for (const address of IPv4.correct) {
        it(`${address}`, () => {
          const ip = new IPAddr(address);

          expect(ip.getRelativePosition(ip, ip.maxHost)).toBeGreaterThanOrEqual(
            0,
          );
          expect(
            ip.minHost.getRelativePosition(ip.minHost, ip.maxHost),
          ).toBeLessThanOrEqual(1);

          expect(
            ip.maxHost.getRelativePosition(ip.minHost, ip.minHost),
          ).toBeGreaterThan(1);
          expect(
            ip.minHost.getRelativePosition(ip.maxHost, ip.maxHost),
          ).toBeLessThan(0);
        });
        break;
      }
    });

    describe("should return IPv6 addresses relative position in range", () => {
      for (const address of IPv6.correct) {
        it(`${address}`, () => {
          const ip = new IPAddr(address);

          expect(ip.getRelativePosition(ip, ip.maxHost)).toBeGreaterThanOrEqual(
            0,
          );
          expect(
            ip.minHost.getRelativePosition(ip.minHost, ip.maxHost),
          ).toBeLessThanOrEqual(1);

          expect(
            ip.maxHost.getRelativePosition(ip.minHost, ip.minHost),
          ).toBeGreaterThan(1);
          expect(
            ip.minHost.getRelativePosition(ip.maxHost, ip.maxHost),
          ).toBeLessThan(0);
        });
        break;
      }
    });
  });

  describe("isValid(...)", () => {
    describe("should throw error on incorrect IPv4 addresses", () => {
      for (const address of IPv4.incorrect) {
        it(`${address}`, () => {
          expect(() => new IPAddr(address)).toThrow();
        });
      }
    });

    describe("should throw error on incorrect IPv6 addresses", () => {
      for (const address of IPv6.incorrect) {
        it(`${address}`, () => {
          expect(() => new IPAddr(address)).toThrow();
        });
      }
    });
  });
});
