/**
 * Class representing an IP address.
 */
export class IPAddr {
  /** @type {Array<string>} */
  static v6cidrs = [
    "1",
    "2",
    "4",
    "8",
    "16",
    "32",
    "64",
    "128",
    "256",
    "512",
    "1024",
    "2048",
    "4096",
    "8192",
    "16384",
    "32768",
    "65536",
    "131072",
    "262144",
    "524288",
    "1048576",
    "2097152",
    "4194304",
    "8388608",
    "16777216",
    "33554432",
    "67108864",
    "134217728",
    "268435456",
    "536870912",
    "1073741824",
    "2147483648",
    "4294967296",
    "8589934592",
    "17179869184",
    "34359738368",
    "68719476736",
    "137438953472",
    "274877906944",
    "549755813888",
    "1099511627776",
    "2199023255552",
    "4398046511104",
    "8796093022208",
    "17592186044416",
    "35184372088832",
    "70368744177664",
    "140737488355328",
    "281474976710656",
    "562949953421312",
    "1125899906842624",
    "2251799813685248",
    "4503599627370496",
    "9007199254740992",
    "18014398509481984",
    "36028797018963968",
    "72057594037927936",
    "144115188075855872",
    "288230376151711744",
    "576460752303423488",
    "1152921504606846976",
    "2305843009213693952",
    "4611686018427387904",
    "9223372036854775808",
    "18446744073709551616",
    "36893488147419103232",
    "73786976294838206464",
    "147573952589676412928",
    "295147905179352825856",
    "590295810358705651712",
    "1180591620717411303424",
    "2361183241434822606848",
    "4722366482869645213696",
    "9444732965739290427392",
    "18889465931478580854784",
    "37778931862957161709568",
    "75557863725914323419136",
    "151115727451828646838272",
    "302231454903657293676544",
    "604462909807314587353088",
    "1208925819614629174706176",
    "2417851639229258349412352",
    "4835703278458516698824704",
    "9671406556917033397649408",
    "19342813113834066795298816",
    "38685626227668133590597632",
    "77371252455336267181195264",
    "154742504910672534362390528",
    "309485009821345068724781056",
    "618970019642690137449562112",
    "1237940039285380274899124224",
    "2475880078570760549798248448",
    "4951760157141521099596496896",
    "9903520314283042199192993792",
    "19807040628566084398385987584",
    "39614081257132168796771975168",
    "79228162514264337593543950336",
    "158456325028528675187087900672",
    "316912650057057350374175801344",
    "633825300114114700748351602688",
    "1267650600228229401496703205376",
    "2535301200456458802993406410752",
    "5070602400912917605986812821504",
    "10141204801825835211973625643008",
    "20282409603651670423947251286016",
    "40564819207303340847894502572032",
    "81129638414606681695789005144064",
    "162259276829213363391578010288128",
    "324518553658426726783156020576256",
    "649037107316853453566312041152512",
    "1298074214633706907132624082305024",
    "2596148429267413814265248164610048",
    "5192296858534827628530496329220096",
    "10384593717069655257060992658440192",
    "20769187434139310514121985316880384",
    "41538374868278621028243970633760768",
    "83076749736557242056487941267521536",
    "166153499473114484112975882535043072",
    "332306998946228968225951765070086144",
    "664613997892457936451903530140172288",
    "1329227995784915872903807060280344576",
    "2658455991569831745807614120560689152",
    "5316911983139663491615228241121378304",
    "10633823966279326983230456482242756608",
    "21267647932558653966460912964485513216",
    "42535295865117307932921825928971026432",
    "85070591730234615865843651857942052864",
    "170141183460469231731687303715884105728",
    "340282366920938463463374607431768211456",
  ];

  /**
   * Create an IP address.
   * @param {string} address The address. Allowed to contain CIDR in slash notation.
   * @param {number} [cidr=0] The CIDR notation.
   */
  constructor(address, cidr = 0) {
    if (!address)
      throw new SyntaxError(
        `Cannot convert ${
          typeof address === "string" ? `'${address}'` : address
        } to IPAddr, Address must be defined`,
      );

    if (typeof address !== "string") {
      throw new SyntaxError(
        `Cannot convert Address ${
          typeof address === "string" ? `'${address}'` : address
        } to IPAddr, must be type of string`,
      );
    }

    if (typeof cidr !== "number") {
      throw new SyntaxError(
        `Cannot convert CIDR ${
          typeof cidr === "string" ? `'${cidr}'` : cidr
        } to IPAddr, must be type of number`,
      );
    }

    this._address = address.toLowerCase();
    this._cidr = cidr;

    this._info = `The provided address '${this._address}' not validated yet.`;
    this._warning = null;
    this._error = null;

    /* Initialize */
    this.initialize();

    /* Validate */
    this._validate();
  }

  /**
   * Initialize the IP address.
   */
  initialize() {
    if (this._address.includes("/")) {
      const [address, cidr] = this._address.split("/");
      this._address = address;
      this._cidr = parseInt(cidr);
    }

    if (this._address.includes(":")) {
      /* Its's an IPv6 address */

      // Expand IPv6 if possible
      const expanded = IPAddr._getExpandedIPv6(this._address);
      this._address = expanded !== null ? expanded : this._address;

      this._version = 6;
      this._maxCIDR = 128;
      if (isNaN(this._cidr)) this._cidr = 128;
    } else if (this._address.length >= 7) {
      /* Its's an IPv4 address */

      this._version = 4;
      this._maxCIDR = 32;
      if (isNaN(this._cidr)) this._cidr = 32;
    }
  }

  /**
   * Get the version of the IP address.
   * @returns {number} The version of the IP address.
   */
  get version() {
    return this._version || 0;
  }

  /**
   * Gets the info of the IP address instance.
   * @returns {string|null} The message, null if there is no info available.
   */
  get info() {
    return this._info;
  }

  /**
   * Gets the warning of the IP address instance.
   * @returns {string|null} The message, null if there is no warning available.
   */
  get warning() {
    return this._warning;
  }

  /**
   * Gets the error of the IP address instance.
   * @returns {string|null} The message, null if there is no error available.
   */
  get error() {
    return this._error;
  }

  /**
   * Gets the CIDR.
   * @returns {number} The CIDR.
   */
  get cidr() {
    return this._cidr;
  }

  /**
   * Gets the address.
   * @returns {string} The address.
   */
  get address() {
    return this._address;
  }

  /**
   * Sets the CIDR.
   * @param {number} value The CIDR value.
   * @throws {Error|SyntaxError}
   */
  set cidr(value) {
    this._validateType(value, "number");

    this._cidr = value;

    /* Validate */
    this._validate();
  }

  /**
   * Gets the wildcard.
   * @returns {IPAddr} The wildcard IP address.
   */
  get wildcard() {
    const bitmask = this.bitmask;
    const inverted = [...bitmask]
      .map((bit) => (bit === "1" ? "0" : "1"))
      .join("");

    switch (this._version) {
      case 6:
        return new IPAddr(IPAddr.getHexColonIPv6FromBinary(inverted));
      case 4:
        return new IPAddr(IPAddr.getDecimalDottedIPv4FromBinary(inverted));
    }
  }

  /**
   * Gets the netmask.
   * @returns {IPAddr} The netmask IP address.
   */
  get netmask() {
    switch (this._version) {
      case 6:
        return new IPAddr(IPAddr.getHexColonIPv6FromBinary(this.bitmask));
      case 4:
        return new IPAddr(IPAddr.getDecimalDottedIPv4FromBinary(this.bitmask));
    }
  }

  /**
   * Gets the network.
   * @returns {IPAddr} The network IP address.
   */
  get network() {
    switch (this._version) {
      case 6:
        return new IPAddr(IPAddr._getNetworkIPv6(this._address, this._cidr));
      case 4:
        return new IPAddr(IPAddr._getNetworkIPv4(this._address, this._cidr));
    }
  }

  /**
   * Gets the 5 CIDR neighbor prefixes: one CIDR up, two side siblings left and right, first and second half down.
   * @returns {Object} An object containing the neighbor prefixes.
   */
  get neighbors() {
    const isIPv4 = this.isIPv4();

    const top = Math.ceil(this.cidr) === 0;
    const bottom = Math.floor(this.cidr) === (isIPv4 ? 32 : 128);

    const leftmost = this.minHost.toBinaryString().indexOf("1") === -1;
    const rightmost = this.maxHost.toBinaryString().indexOf("0") === -1;

    let neighbors = {
      up: null,
      left: null,
      right: null,
      downleft: null,
      downright: null,
    };

    if (!top) {
      let ipAddr = this.minHost;

      ipAddr.cidr = Math.ceil(this.cidr) - 1;
      neighbors.up = ipAddr.toString();
    }

    if (!leftmost) {
      let ipAddr = this.minHost.subtract(
        new IPAddr(isIPv4 ? "0.0.0.1" : "0::1"),
      );

      ipAddr.cidr = Math.ceil(this.cidr);
      neighbors.left = ipAddr.toString();
    }

    if (!rightmost) {
      let ipAddr = this.maxHost.add(new IPAddr(isIPv4 ? "0.0.0.1" : "0::1"));

      ipAddr.cidr = Math.ceil(this.cidr);
      neighbors.right = ipAddr.toString();
    }

    if (!bottom) {
      const cidr = Math.min(isIPv4 ? 32 : 128, Math.floor(this.cidr) + 1);

      // Left
      let ipAddrLeft = this.minHost;

      ipAddrLeft.cidr = cidr;
      neighbors.downleft = ipAddrLeft.toString();

      // Right
      let ipAddrRight = this.maxHost;

      ipAddrRight.cidr = cidr;
      neighbors.downright = ipAddrRight.toString();
    }

    return neighbors;
  }

  /**
   * Gets the bitmask.
   * @returns {string} The binary mask.
   */
  get bitmask() {
    return IPAddr._expandCIDR(this.cidr, this._maxCIDR);
  }

  /**
   * Gets the host count.
   * @returns {BigInt} The host count.
   */
  get hostCount() {
    switch (this._version) {
      case 6:
        return BigInt(IPAddr.v6cidrs[parseInt(128 - this._cidr)]);
      case 4:
        return BigInt(Math.pow(2, parseInt(32 - this._cidr)).toString());
    }
  }

  /**
   * Gets the maximum host IP address for the IP address.
   * @returns {IPAddr} The maximum host IP address.
   */
  get maxHost() {
    switch (this._version) {
      case 6:
        return this.maxHostIPv6;
      case 4:
        return this.maxHostIPv4;
      default:
        return "";
    }
  }

  /**
   * Gets the minimum host IP address for the IP address.
   * @returns {IPAddr} The minimum host IP address.
   */
  get minHost() {
    switch (this._version) {
      case 6:
        return this.minHostIPv6;
      case 4:
        return this.minHostIPv4;
    }
  }

  /**
   * Gets the minimum host address for IPv4.
   * @returns {IPAddr} The minimum host IP address.
   */
  get minHostIPv4() {
    this._validateVersionConsistency(4);

    const quads = this.network.address.split(".");

    if (this._cidr !== 32) quads[3]++;

    return new IPAddr(quads.join("."));
  }

  /**
   * Gets the maximum host address for IPv4.
   * @returns {IPAddr} The maximum host IP address.
   */
  get maxHostIPv4() {
    this._validateVersionConsistency(4);

    const quads = this.broadcast.address.split(".");

    if (this._cidr !== 32) quads[3]--;

    return new IPAddr(quads.join("."));
  }

  /**
   * Gets the minimum host address for IPv6.
   * @returns {IPAddr} The minimum host IP address.
   */
  get minHostIPv6() {
    this._validateVersionConsistency(6);

    let chunks = this.network.address.split(":");

    chunks[7] = 1 + parseInt(chunks[7], 16);
    chunks[7] = chunks[7].toString(16);

    return new IPAddr(IPAddr._getExpandedIPv6(chunks.join(":")));
  }

  /**
   * Gets the maximum host address for IPv6.
   * @returns {IPAddr} The maximum host IP address.
   */
  get maxHostIPv6() {
    this._validateVersionConsistency(6);

    let chunks = this.expanded.split(":");
    let mask_chunks = this.wildcard.address.split(":");
    let outp = [];

    for (let i = 0; i <= 7; i++) {
      let chunk = parseInt(chunks[i], 16) | parseInt(mask_chunks[i], 16);
      outp.push(chunk.toString(16));
    }

    return new IPAddr(IPAddr._getExpandedIPv6(outp.join(":")).toUpperCase());
  }

  /**
   * Gets the broadcast address.
   * @returns {IPAddr|null} The broadcast IP address, and null when IP address version is not 4.
   */
  get broadcast() {
    switch (this._version) {
      case 4: {
        var ip_quads = this.address.split(".");
        var mask_quads = this.wildcard.address.split(".");
        var outp = [];

        for (var i = 0; i <= 3; i++) outp.push(ip_quads[i] | mask_quads[i]);

        return new IPAddr(outp.join("."));
      }
      default: {
        return null;
      }
    }
  }

  /**
   * Gets the expanded IPv4/IPv6 address.
   * @returns {string} The expanded IPv4/IPv6 address.
   */
  get expanded() {
    switch (this._version) {
      case 6:
        return IPAddr._getExpandedIPv6(this._address);
      case 4:
        return IPAddr._getExpandedIPv4(this._address);
    }
  }

  /**
   * Gets the compressed IP address.
   * @returns {string} The compressed IPv6 IP address.
   */
  get compressed() {
    switch (this._version) {
      case 6: {
        // First remove the leading 0s of the octets. If it's '0000', replace with '0'
        let output = this._address
          .split(":")
          .map((terms) => terms.replace(/\b0+/g, "") || "0")
          .join(":");

        // Then search for all occurrences of continuous '0' octets
        let zeros = [...output.matchAll(/\b:?(?:0+:?){2,}/g)];

        // If there are occurences, see which is the longest one and replace it with '::'
        if (zeros.length > 0) {
          let max = "";
          zeros.forEach((item) => {
            if (
              item[0].replaceAll(":", "").length >
              max.replaceAll(":", "").length
            ) {
              max = item[0];
            }
          });
          output = output.replace(max, "::");
        }
        return output;
      }
      default:
        return this._address;
    }
  }

  /**
   * Gets mapped IPv6 address.
   * @returns {IPAddr} The new IPv6 mapped address instance.
   */
  get mappedIPv6() {
    switch (this._version) {
      case 4: {
        const chunks = ("::FFFF:" + this._address).match(
          /^([0:]+:FFFF:)([\d.]+)$/i,
        );
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

        return new IPAddr(mapped, this._cidr + 96);
      }
      case 6: {
        // It's already an IPv6 address, nothing to do.
        return new IPAddr(this._address, this._cidr);
      }
    }
  }

  /**
   * Sum two IP addresses.
   * @param {IPAddr} other The other IP address to add to the current one.
   * @returns {IPAddr} Sum of IP addresses.
   * @throws {SyntaxError}
   */
  add(other) {
    this._validateType(other, "IPAddr");
    this._validateVersionEquality(this, other);

    switch (this.version) {
      case 4: {
        const sum = IPAddr._addIPIntegers(
          this.toBinaryArray(),
          other.toBinaryArray(),
        );
        const result = IPAddr.fromOctets(sum);
        result.cidr = this._cidr;

        return result;
      }
      case 6: {
        const sum = IPAddr._addIPIntegers(
          this.toBinaryArray(),
          other.toBinaryArray(),
        );
        const result = IPAddr.fromSegments(sum);
        result.cidr = this._cidr;

        return result;
      }
    }
  }

  /**
   * Adds two IP integers, supporting both IPv4 and IPv6.
   * @private
   * @param {number[]|number[]} a First IP integer or array of octets/segments.
   * @param {number[]|number[]} b Second IP integer or array of octets/segments.
   * @returns {number[]|-1} Sum of the two IP integers, or -1 for type mismatch.
   */
  static _addIPIntegers(a, b) {
    const addInts = (a, b, carry, base) => {
      const sum = a + b + carry;
      return [sum % base, Math.floor(sum / base)];
    };

    if (Array.isArray(a) && Array.isArray(b)) {
      if (a.length !== b.length) return -1;

      const result = [];
      let carry = 0;
      const base = a.length === 4 ? 256 : 0x10000; // 256 for IPv4 octets, 65536 for IPv6 segments

      for (let i = a.length - 1; i >= 0; i--) {
        const [sum, newCarry] = addInts(a[i], b[i], carry, base);
        result.unshift(sum);
        carry = newCarry;
      }

      if (carry > 0) {
        this._warning =
          "Addition resulted in a carry out of the most significant bit, wrapping around";
      }

      return result;
    }

    return -1; // or handle type mismatch differently
  }

  /**
   * Subtract another IP address from the current one.
   * @param {IPAddr} other The other IP address to subtract from the current one.
   * @returns {IPAddr} Difference of IP addresses.
   * @throws {SyntaxError}
   */
  subtract(other) {
    this._validateType(other, "IPAddr");
    this._validateVersionEquality(this, other);

    switch (this.version) {
      case 4: {
        const diff = IPAddr._subtractIPIntegers(
          this.toBinaryArray(),
          other.toBinaryArray(),
        );
        const result = IPAddr.fromOctets(diff);
        result.cidr = this._cidr;

        return result;
      }
      case 6: {
        const diff = IPAddr._subtractIPIntegers(
          this.toBinaryArray(),
          other.toBinaryArray(),
        );
        const result = IPAddr.fromSegments(diff);
        result.cidr = this._cidr;

        return result;
      }
    }
  }

  /**
   * Subtracts two IP integers, supporting both IPv4 and IPv6.
   * @private
   * @param {number[]|number[]} a Minuend IP integer or array of octets/segments.
   * @param {number[]|number[]} b Subtrahend IP integer or array of octets/segments.
   * @returns {number[]|-1} Difference of the two IP integers, or -1 for type mismatch.
   */
  static _subtractIPIntegers(a, b) {
    const subInts = (a, b, borrow, base) => {
      const diff = a - b - borrow;
      return [diff >= 0 ? diff : base + diff, diff < 0 ? 1 : 0];
    };

    if (Array.isArray(a) && Array.isArray(b)) {
      if (a.length !== b.length) return -1;

      const result = [];
      let borrow = 0;
      const base = a.length === 4 ? 256 : 0x10000; // 256 for IPv4 octets, 65536 for IPv6 segments

      for (let i = a.length - 1; i >= 0; i--) {
        const [diff, newBorrow] = subInts(a[i], b[i], borrow, base);
        result.unshift(diff);
        borrow = newBorrow;
      }

      if (borrow > 0) {
        this._warning =
          "Subtraction resulted in a negative value, wrapping around";
      }

      return result;
    }

    return -1; // or handle type mismatch differently
  }

  /**
   * Compares another IP address to the current one.
   * @param {IPAddr} other The other IP address to compare to the current one.
   * @returns {number} Returns -1 if a<b, 1 if a>b, 0 if a==b.
   * @throws {SyntaxError}
   */
  compare(other) {
    this._validateType(other, "IPAddr");
    this._validateVersionEquality(this, other);

    // Compare IPs
    return compareArrays(this.toBinaryArray(), other.toBinaryArray());

    // Helper function to compare two arrays of numbers
    function compareArrays(arrA, arrB) {
      for (let i = 0; i < arrA.length; i++) {
        if (arrA[i] < arrB[i]) return -1;
        if (arrA[i] > arrB[i]) return 1;
      }
      return 0; // Arrays are equal
    }
  }

  /**
   * Calculates the start and end percentage of overlap between the current IP block (parent) and another IP block (child).
   * @param {IPAddr} other The child IP block to compare with the current IP block.
   * @returns {number[]} An array containing the start and end percentage of overlap between the parent and child blocks. Values <0 or >100 are possible if the blocks only partially overlap or do not overlap at all. Percentages are relative to the parent block.
   * @throws {SyntaxError}
   */
  overlap(other) {
    this._validateType(other, "IPAddr");
    this._validateVersionEquality(this, other);

    const minHostParent = this.minHost;
    const maxHostParent = this.maxHost;
    const minHostChild = other.minHost;
    const maxHostChild = other.maxHost;

    const startOverlapPercentage =
      minHostChild.getRelativePosition(minHostParent, maxHostParent) * 100;
    const endOverlapPercentage =
      maxHostChild.getRelativePosition(minHostParent, maxHostParent, true) *
      100;

    return [startOverlapPercentage, endOverlapPercentage];
  }

  /**
   * Gets expanded IPv4 address to its full notation.
   * @private
   * @param {string} ip The IPv4 address.
   * @returns {string} The expanded IPv4 address.
   */
  static _getExpandedIPv4(ip) {
    const quads = ip.split(".");
    const outp = [];

    for (let i = 0; i < quads.length; i++) {
      outp.push("0".repeat(3 - quads[i].length) + quads[i]);
    }

    return outp.join(".");
  }

  /**
   * Gets expanded IPv6 address to its full notation.
   * @private
   * @param {string} ip The IPv6 address to expand.
   * @returns {string|null} The expanded IPv6 address or an empty string if invalid, and null if IPv6 address is invalid.
   */
  static _getExpandedIPv6(ip) {
    const colons = (ip.match(/:/g) || []).length;

    if (colons > 7) {
      this._error = "The IPv6 address contains too many colons.";
      return null;
    }

    const missingColons = 8 - colons;

    if (/:::/.test(ip)) {
      this._error = "The IPv6 address contains an invalid group of colons.";
      return null;
    }

    ip = ip.replace(/^::/, "0000::").replace(/::$/, "::0000");

    if (ip.startsWith(":")) {
      this._error = "The IPv6 address should not start with a colon.";
      return null;
    } else if (ip.endsWith(":")) {
      this._error = "The IPv6 address should not end with a colon.";
      return null;
    }

    ip = ip.replace(/::/g, ":" + "0000:".repeat(missingColons));

    const oldChunks = ip.split(/:/);
    const newChunks = oldChunks.map((chunk) => {
      if (chunk.includes("00000")) {
        this._error = "The IPv6 address contains too many zeroes in a segment.";
        return null;
      }

      if (chunk === "0") chunk = "";

      return chunk.padStart(4, "0");
    });

    return newChunks.join(":");
  }

  /**
   * Gets the IPv4 network address.
   * @private
   * @param {string} ip The IPv4 address.
   * @param {number} cidr The CIDR value.
   * @returns {string} The IPv4 network IP address.
   */
  static _getNetworkIPv4(ip, cidr) {
    const v4 = ip.split(".");
    const mask = IPAddr.getDecimalDottedIPv4FromBinary(
      IPAddr._expandCIDR(cidr, 32),
    ).split(".");
    const outp = v4.map((quad, i) => quad & mask[i]);

    return outp.join(".");
  }

  /**
   * Gets the IPv6 network address.
   * @private
   * @param {string} ip The IPv6 address.
   * @param {number} cidr The CIDR value.
   * @returns {string} The IPv6 network IP address.
   */
  static _getNetworkIPv6(ip, cidr) {
    const v6 = IPAddr._getExpandedIPv6(ip).split(":");
    const mask = IPAddr.getHexColonIPv6FromBinary(
      IPAddr._expandCIDR(cidr, 128),
    ).split(":");

    const outp = v6.map((segment, i) =>
      (parseInt(segment, 16) & parseInt(mask[i], 16))
        .toString(16)
        .toUpperCase(),
    );

    return IPAddr._getExpandedIPv6(outp.join(":"));
  }

  /**
   * Creates an {@link IPAddr} instance from an array of IPv4 octet integers.
   * @param {number[]} octets The array of decimal octet values.
   * @returns {IPAddr} A new IP address instance.
   */
  static fromOctets(octets) {
    const binary = octets.map((octet) => IPAddr.octetToBinary(octet)).join("");

    return IPAddr.fromBinary(binary);
  }

  /**
   * Creates an {@link IPAddr} instance from an array of IPv6 segments.
   * @param {number[]} segments The array of decimal segment values.
   * @returns {IPAddr} A new IP address instance.
   */
  static fromSegments(segments) {
    const binaryStr = segments
      .map((segment) => segment.toString(2).padStart(16, "0"))
      .join("");

    return IPAddr.fromBinary(binaryStr);
  }

  /**
   * Creates an {@link IPAddr} instance from an IPv4 or IPv6 binary string.
   * @param {string} binary The binary string of a IPv4 (up to 32 bit) or IPv6 (up to 128 bit) address.
   * @returns {IPAddr} A new IP address instance based on the binary string.
   */
  static fromBinary(binary) {
    const isIPv4 = binary.length <= 32;
    const size = isIPv4 ? 8 : 16;

    let ipStr = "";
    let bits = 0;

    while (binary.length) {
      ipStr += parseInt(binary.slice(0, size), 2).toString(isIPv4 ? 10 : 16);
      binary = binary.slice(size);
      bits += 1;

      // Force break if maximum possible 128 bits is reached.
      if (bits === 128) break;

      if (binary.length) {
        ipStr += isIPv4 ? "." : ":";
      }
    }

    return new IPAddr(ipStr, this._cidr);
  }

  /**
   * Creates an {@link IPAddr} instance from an IP range string.
   * @param {string} range The IP range string to convert.
   * @returns {IPAddr} An IPAddr instance representing the given range.
   * @throws {Error} Throws an error if the range is not in the correct format.
   */
  static fromRange(range) {
    // Validate and extract IPs from the range string
    const ipv4RangeRegex =
      /^(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})-(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})$/;
    const ipv6RangeRegex = /^([0-9a-fA-F:]+)-([0-9a-fA-F:]+)$/;

    let match = range.match(ipv4RangeRegex);
    let isIPv6 = false;

    if (!match) {
      match = range.match(ipv6RangeRegex);
      isIPv6 = true;
    }

    if (!match) {
      throw new Error(
        `Invalid IP range format '${range}'. Range must be in the format "startIP-endIP".`,
      );
    }

    const [_, startIP, endIP] = match;

    const start = new IPAddr(startIP).toBinaryString();
    const end = new IPAddr(endIP).toBinaryString();

    // Calculate CIDR from the IP range
    let cidr;

    if (isIPv6) {
      cidr = calculateCIDRFromRangeIPv6(start, end);
    } else {
      cidr = calculateCIDRFromRangeIPv4(start, end);
    }

    if (cidr === -1) {
      throw new Error(
        `Invalid IP range '${range}'. The range cannot be represented by a single CIDR block.`,
      );
    }

    // Create and return the IPAddr instance
    return new IPAddr(startIP, cidr);

    function calculateCIDRFromRangeIPv4(start, end) {
      let prefixLength = 0;

      while (
        prefixLength < start.length &&
        start[prefixLength] === end[prefixLength]
      ) {
        prefixLength++;
      }

      const startSuffix = start.substring(prefixLength);
      const endSuffix = end.substring(prefixLength);

      // Check if startSuffix is all zeros and endSuffix is all ones
      if (!/^0+$/.test(startSuffix) || !/^1+$/.test(endSuffix)) {
        return -1; // Cannot be represented by a single CIDR
      }

      return prefixLength;
    }

    function calculateCIDRFromRangeIPv6(start, end) {
      let prefixLength = 0;

      while (
        prefixLength < start.length &&
        start[prefixLength] === end[prefixLength]
      ) {
        prefixLength++;
      }

      const startSuffix = start.substring(prefixLength);
      const endSuffix = end.substring(prefixLength);

      // Check if startSuffix is all zeros and endSuffix is all ones
      if (!/^0+$/.test(startSuffix) || !/^1+$/.test(endSuffix)) {
        return -1; // Cannot be represented by a single CIDR
      }

      return prefixLength;
    }
  }

  /**
   * Convert binary to hexadecimal.
   * @param {string} bin The binary string.
   * @returns {string} The hexadecimal representation.
   */
  static binaryToHexadecimal(bin) {
    let outp = parseInt(this.binaryToDecimal(bin), 10)
      .toString(16)
      .toUpperCase();
    return "0".repeat(4 - outp.length) + outp;
  }

  /**
   * Convert binary to decimal.
   * @param {string} bin The binary string.
   * @param {number} [len] The length of the output string.
   * @returns {string} The decimal representation.
   */
  static binaryToDecimal(bin, len) {
    let outp = [...bin]
      .reduce(
        (acc, bit, i) =>
          acc + (bit === "1" ? Math.pow(2, bin.length - 1 - i) : 0),
        0,
      )
      .toString();
    if (!len) return outp;
    return "0".repeat(len - outp.length) + outp;
  }

  /**
   * Convert octet (of an IPv4 address) to 8-bit binary representation.
   * @param {number} octet The octet number.
   * @returns {string} The binary representation.
   */
  static octetToBinary(octet) {
    let binaryStr = octet.toString(2);

    // Pad with zeros to get 8-bit binary representation!
    while (binaryStr.length < 8) {
      binaryStr = "0" + binaryStr;
    }

    return binaryStr;
  }

  /**
   * Convert segment (of an IPv6 address) to 32-bit binary representation.
   * @param {number[]} segment The segment number.
   * @returns {string} The binary representation.
   */
  static segmentToBinary(segment) {
    /**
     * Convert number to 32-bit binary representation.
     * @param {number} num The number.
     * @returns {string} The 32-bit binary representation.
     */
    function numToBin32(num) {
      let binaryStr = num.toString(2);

      // Pad with zeros!
      while (binaryStr.length < 32) {
        binaryStr = "0" + binaryStr;
      }

      return binaryStr;
    }

    let binaryStr = "";

    for (const number of segment) {
      binaryStr += numToBin32(number);
    }

    return binaryStr;
  }

  /**
   * Converts a binary string representation of an IPv4 address into its decimal dotted-quad format.
   * @param {string} bitmask Binary string representation of an IPv4 address.
   * @returns {string} Decimal dotted-quad format of the IPv4 address.
   */
  static getDecimalDottedIPv4FromBinary(bitmask) {
    if (!bitmask) return "";

    const chunks = [];

    while (bitmask) {
      chunks.push(parseInt(bitmask.slice(0, 8), 2)); // Convert binary to decimal
      bitmask = bitmask.slice(8);
    }

    while (chunks.length < 4) {
      chunks.push(0); // Pad with zeros if necessary
    }

    return chunks.join(".");
  }

  /**
   * Converts a binary string representation of an IPv6 address into its hexadecimal colon-separated format.
   * @param {string} bitmask Binary string representation of an IPv6 address.
   * @returns {string} Hexadecimal colon-separated format of the IPv6 address.
   */
  static getHexColonIPv6FromBinary(bitmask) {
    if (!bitmask) return "";

    const chunks = [];

    while (bitmask) {
      chunks.push(parseInt(bitmask.slice(0, 16), 2).toString(16)); // Convert binary to hexadecimal
      bitmask = bitmask.slice(16);
    }

    while (chunks.length < 8) {
      chunks.push("0"); // Pad with zeros if necessary
    }

    return chunks.join(":");
  }

  /**
   * Get string representation of the IP address.
   * @param {string} [format="cidr"] The string formatting.
   * @returns {string} The string representation.
   * @throws {SyntaxError}
   */
  toString(format = "cidr") {
    this._validateType(format, "string");

    switch (format) {
      case "cidr":
        return `${this.address}/${this._cidr}`;
      case "range":
        return `${this.address}-${
          this.isIPv4() ? this.broadcast.address : this.maxHost.address
        }`;
    }
  }

  /**
   * Get binary string representation of IP address.
   * @returns {string} The binary string representation.
   */
  toBinaryString() {
    const isIPv4 = this.isIPv4();
    const segments = this.expanded.split(isIPv4 ? "." : ":");

    let binaryString = "";

    for (var segment of segments) {
      var segmentInteger = parseInt(segment, isIPv4 ? 10 : 16);
      var segmentBinary = segmentInteger.toString(2);

      // Pad with zeros!
      while (segmentBinary.length < (isIPv4 ? 8 : 16)) {
        segmentBinary = "0" + segmentBinary;
      }

      binaryString += segmentBinary;
    }

    return binaryString;
  }

  /**
   * Get binary array representation of IP address.
   * @returns {Array<number>} The binary array representation.
   */
  toBinaryArray() {
    const binaryStr = this.toBinaryString();

    switch (this._version) {
      case 4: {
        return [
          parseInt(binaryStr.slice(0, 8), 2),
          parseInt(binaryStr.slice(8, 16), 2),
          parseInt(binaryStr.slice(16, 24), 2),
          parseInt(binaryStr.slice(24, 32), 2),
        ];
      }
      case 6: {
        const segments = [];

        for (let i = 0; i < 8; i++) {
          segments.push(parseInt(binaryStr.slice(i * 16, (i + 1) * 16), 2));
        }

        return segments;
      }
    }
  }

  /**
   * Get Integer representation of IP address.
   * @returns {BigInt} The BigInt representation.
   */
  toInteger() {
    switch (this._version) {
      case 4: {
        return BigInt(
          this._address
            .split(".")
            .reduce((int, octet) => (int << 8) + parseInt(octet, 10), 0) >>> 0,
        );
      }
      case 6: {
        return BigInt(`0x${this._address.replace(/:/g, "")}`);
      }
    }
  }

  /**
   * Calculates the relative position of the current IP within the specified IP range.
   * @param {IPAddr} start The start IP of the range.
   * @param {IPAddr} end The end IP of the range.
   * @param {boolean} [after=false] Whether to calculate the position for right before (false) or right after (true) the IP address.
   * @returns {number} The relative position of the IP within the range (0 to 1). Values of <0 or >1 are possible if the IP is outside of the range.
   * @throws {SyntaxError}
   */
  getRelativePosition(start, end, after = false) {
    this._validateType(start, "IPAddr");
    this._validateType(end, "IPAddr");
    this._validateType(after, "boolean");
    this._validateVersionEquality(start, end);

    /**
     * Calculates the percentage value of the position within a range.
     * @param {number} position The position within the range.
     * @param {number} start The start of the range.
     * @param {number} end The end of the range.
     * @returns {number} The percentage value of the position within the range.
     */
    function calculatePercentage(position, start, end) {
      return (position - start) / (end + 1 - start);
    }

    const binaryArr = this.toBinaryArray();
    const first = start.toBinaryArray();
    const last = end.toBinaryArray();

    if (this.version !== start.version || this.version !== end.version) {
      /* Different IP address Families */
      return null;
    }

    switch (this._version) {
      case 4: {
        const position =
          binaryArr.reduce((acc, val) => acc * 256 + val, 0) + (after ? 1 : 0);
        const startVal = first.reduce((acc, val) => acc * 256 + val, 0);
        const endVal = last.reduce((acc, val) => acc * 256 + val, 0);

        const percentage = calculatePercentage(position, startVal, endVal);

        return percentage;
      }
      case 6: {
        let weight = 1;
        let amount = 0;
        let percentage = 0;

        for (let i = 0; i < first.length; i++) {
          const segmentPercentage = calculatePercentage(
            binaryArr[i] + (after && i === first.length - 1 ? 1 : 0),
            first[i],
            last[i],
          );

          weight /= amount + 1;
          amount = last[i] - first[i];
          percentage += segmentPercentage * weight;
        }

        return percentage;
      }
    }
  }

  /**
   * Calculates the IP address at a given percentage within the minimum and maximum Host addresses of the current IP address.
   * @param {number} percentage The percentage at which to calculate the IP address.
   * @returns {string} The calculated IP address at the given percentage.
   * @throws {Error} Throws an error if the IP addresses have mismatching versions.
   */
  getAddressAtPercentage(percentage) {
    this._validateType(percentage, "number");

    const start = this.minHost;
    const end = this.maxHost;

    const first = start.toBinaryArray();
    const last = end.toBinaryArray();

    const ipNum = start.isIPv4()
      ? calculateIPv4Num(percentage, first, last)
      : calculateIPv6Num(percentage, first, last);

    return start.isIPv4()
      ? IPAddr.fromOctets(ipNum)
      : IPAddr.fromSegments(ipNum);

    function calculateIPv4Num(perc, start, end) {
      const num = start + Math.round((end - start) * (perc / 100));
      return Math.max(start, Math.min(end, num));
    }

    function calculateIPv6Num(perc, start, end) {
      return start.map((val, idx) => {
        const num = val + Math.round((end[idx] - val) * (perc / 100));
        return Math.max(val, Math.min(end[idx], num));
      });
    }
  }

  /**
   * Divides the current IP address into an array of smaller prefixes of a specified CIDR size.
   * If the specified CIDR size is larger or equal to the original prefix's CIDR, the function returns an array containing only the original prefix.
   *
   * @param {number} targetCidrSize The CIDR size of the resulting smaller prefixes.
   * @returns {IPAddr[]} An array of IPAddr instances representing the smaller prefixes.
   * @throws {Error} Throws an error if targetCidrSize is not a positive integer or if it's smaller than the original prefix's CIDR.
   */
  getUniformSubnets(targetCidrSize) {
    if (
      typeof targetCidrSize !== "number" ||
      targetCidrSize <= 0 ||
      !Number.isInteger(targetCidrSize)
    ) {
      throw new Error(
        "Invalid target CIDR size: It must be a positive integer.",
      );
    }

    const originalCidrSize = parseInt(this.cidr);

    if (originalCidrSize >= targetCidrSize) {
      return [this];
    }

    const subnetIncrement = new IPAddr(this.isIPv4() ? "0.0.0.1" : "0::1");
    const subnets = [];
    let currentIp = new IPAddr(this.address);

    for (
      let i = 0, count = Math.pow(2, targetCidrSize - originalCidrSize);
      i < count;
      i++
    ) {
      const subnet = new IPAddr(currentIp.address, targetCidrSize);
      subnets.push(subnet);
      currentIp = new IPAddr(
        subnet.isIPv4() ? subnet.broadcast.address : subnet.MaxHost(),
      ).add(subnetIncrement);
    }

    return subnets;
  }

  /**
   * Expand the CIDR value.
   * @private
   * @param {number} val The CIDR value.
   * @param {number} max The maximum value.
   * @returns {string} The expanded CIDR value.
   */
  static _expandCIDR(val, max) {
    return "1".repeat(parseInt(val)) + "0".repeat(max - val);
  }

  /**
   * Invert the CIDR value.
   * @private
   * @param {number} val The CIDR value.
   * @param {number} max The maximum value.
   * @returns {string} The inverted CIDR value.
   */
  static _invertCIDR(val, max) {
    return "0".repeat(parseInt(val)) + "1".repeat(max - val);
  }

  /**
   * Check if the IP address is IPv4.
   * @returns {boolean} True if IPv4, false otherwise.
   */
  isIPv4() {
    return this._version === 4;
  }

  /**
   * Check if the IP address is IPv6.
   * @returns {boolean} True if IPv6, false otherwise.
   */
  isIPv6() {
    return this._version === 6;
  }

  /**
   * Raises Error on {@link IPAddr} instance.
   * @private
   * @param {ErrorConstructor|SyntaxErrorConstructor} [constructor=Error] The error constructor.
   * @returns {void}
   * @throws {Error|SyntaxError} Throws an error with message of instance.
   */
  _raise(constructor = Error) {
    throw new constructor(this._error);
  }

  /**
   * Validates {@link IPAddr} instance.
   * @private
   * @returns {void}
   * @throws {Error} Throws an error with message when validation failed.
   */
  _validate() {
    if (!this.isValid()) {
      this._raise();
    } else if (this.isReserved()) {
      this._info = `The provided address '${this._address}' is a valid reserved IPv${this._version} address.`;
    } else {
      this._info = `The provided address '${this._address}' is a valid IPv${this._version} address.`;
    }
  }

  /**
   * Validates type of passed in value.
   * @private
   * @param {any} value The value to validate.
   * @param {string} expectedType The expected type of the passed in value.
   * @returns {void}
   * @throws {SyntaxError} Throws a Sytax Error when the validation failed.
   */
  _validateType(value, expectedType) {
    if (expectedType === "IPAddr" && value.constructor.name === "IPAddr")
      return;
    if (typeof value !== expectedType) {
      this._error = `Invalid value ${
        typeof value === "string" ? `'${value}'` : value
      }, must be of type ${expectedType}`;
      this._raise(SyntaxError);
    }
  }

  /**
   * Validates version equality of IP addresses.
   * @private
   * @param {IPAddr} ipAddrA The first IP address.
   * @param {IPAddr} ipAddrB The second IP address.
   * @returns {void}
   * @throws {SyntaxError} Throws a Syntax Error when the versions do not match.
   */
  _validateVersionEquality(ipAddrA, ipAddrB) {
    if (ipAddrA.version !== ipAddrB.version) {
      this._error = `Invalid version mismatch, IP address versions must be equal.`;
      this._raise(SyntaxError);
    }
  }

  /**
   * Validates version consistency by checking against expected version.
   * @private
   * @param {IPAddr} expectedVersion The expected IP address version.
   * @returns {void}
   * @throws {SyntaxError} Throws a Syntax Error when the versions are not consistent.
   */
  _validateVersionConsistency(expectedVersion) {
    if (this._version !== expectedVersion) {
      this._error = `Inconsistent IP address versions, IP address versions must be equal.`;
      this._raise();
    }
  }

  /**
   * Check if an IPv6 address is reserved.
   * @returns {boolean} True if the address is a reserved IPv6 address, false otherwise.
   */
  isReservedIPv6() {
    if (this._version !== 6) return false;

    // Assuming this._address is a string representation of an IPv6 address
    const address = this._address.toLowerCase();

    // Reserved IPv6 address ranges
    const reservedRanges = [
      {
        start: "0000:0000:0000:0000:0000:0000:0000:0000",
        end: "0000:0000:0000:0000:0000:0000:0000:0000",
      }, // Unspecified address
      {
        start: "0000:0000:0000:0000:0000:0000:0000:0001",
        end: "0000:0000:0000:0000:0000:0000:0000:0001",
      }, // Loopback address
      {
        start: "fc00:0000:0000:0000:0000:0000:0000:0000",
        end: "fdff:ffff:ffff:ffff:ffff:ffff:ffff:ffff",
      }, // Unique local addresses
      {
        start: "fe80:0000:0000:0000:0000:0000:0000:0000",
        end: "febf:ffff:ffff:ffff:ffff:ffff:ffff:ffff",
      }, // Link-local addresses
      // Add more reserved ranges as needed
    ];

    // Convert IPv6 address to a comparable format
    const expandIPv6Address = (address) => {
      const fullAddress = address.replace(/::/g, ":").split(":");
      while (fullAddress.length < 8) {
        fullAddress.splice(fullAddress.indexOf(""), 0, "0000");
      }
      return fullAddress.map((group) => group.padStart(4, "0")).join(":");
    };

    const expandedAddress = expandIPv6Address(address);

    // Check if the address falls within any reserved range
    for (const range of reservedRanges) {
      if (expandedAddress >= range.start && expandedAddress <= range.end) {
        this._warning = `The provided IP address '${this._address}' belongs to a reserved range.`;
        return true;
      }
    }

    return false;
  }

  /**
   * Check if an IPv4 address is reserved.
   * @returns {boolean} True if the address is a reserved IPv4 address, false otherwise.
   */
  isReservedIPv4() {
    if (this._version !== 4) return false;

    const octet = this._address.split(/\./);

    if (224 <= octet[0] && octet[0] <= 239) {
      this._warning = `The provided IP address '${this._address}' belongs to the reserved multicast range.`;
      return false;
    } else if (octet[0] === 0) {
      this._warning = `The provided IP address '${this._address}' belongs to the reserved self-identification range.`;
      return false;
    } else if (octet[0] === 127) {
      this._warning = `The provided IP address '${this._address}' belongs to the reserved loopback range.`;
      return false;
    } else if (
      octet[0] === 255 &&
      octet[1] === 255 &&
      octet[2] === 255 &&
      octet[3] === 255
    ) {
      this._warning = `The provided IP address '${this._address}' is reserved for local broadcast.`;
      return false;
    }

    return true;
  }

  /**
   * Check if the IP address is reserved.
   * @returns {boolean} True if reserved, false otherwise.
   */
  isReserved() {
    if (!isNaN(this._reserved)) return !!this._reserved;

    if (this.isReservedIPv4() && this.isReservedIPv6())
      return (this._reserved = true);

    return (this._reserved = false);
  }

  /**
   * Validates the CIDR notation.
   * @returns {boolean} True if the CIDR notation is valid, false otherwise.
   */
  isValidCIDR() {
    if (!isNaN(this._validCIDR)) return this._validCIDR;

    if (isNaN(this._cidr) || this._cidr < 0 || this._maxCIDR < this._cidr) {
      this._error = `The CIDR block '${this._cidr}' is invalid. It should be a number between 0 and the maximum allowed CIDR size for the IP version.`;
      return (this._validCIDR = false);
    }

    return (this._validCIDR = true);
  }

  /**
   * Validate an IPv6 address.
   * @returns {boolean} True if the address is a valid IPv6 address, false otherwise.
   */
  isValidIPv6() {
    if (this._version !== 6 || !this._address.includes(":")) {
      return false;
    }

    if (!this._address.match(/^[0-9A-F:.]+$/i)) {
      this._error = `The IPv6 address '${this._address}' contains invalid characters. It should only contain hexadecimal digits and colons.`;
      return false;
    }

    if (this._address.match(/^.+:FFFF:\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$/i)) {
      this._address = IPAddr._getExpandedIPv6(this._address);
      if (!this._address) {
        this._error = `The IPv6 address '${this._address}' could not be expanded.`;
        return false;
      }
    }

    // Force consistent case
    const ip = IPAddr._getExpandedIPv6(this._address.toUpperCase());
    if (!ip) {
      this._error = `The IPv6 address '${this._address}' could not be expanded.`;
      return false;
    }

    if (!ip.match(/^(?:[A-F0-9]{4}:){7}[A-F0-9]{4}$/)) {
      this._error = `The IPv6 address '${this._address}' has an invalid syntax.`;
      return false;
    }

    return true;
  }

  /**
   * Validate an IPv4 address.
   * @returns {boolean} True if the address is a valid IPv4 address, false otherwise.
   */
  isValidIPv4() {
    if (this._version !== 4 || !this._address.includes(".")) {
      return false;
    }

    if (this._address.length < 7) {
      this._error = `The provided IP address '${this._address}' is too short to be valid.`;
      return false;
    }

    if (!this._address.match(/^[\d.]+$/)) {
      this._error = `The IP address '${this._address}' should only contain digits and dots.`;
      return false;
    }

    const octet = this._address.split(/\./);

    if (octet.length !== 4) {
      this._error = `A valid IPv4 address '${this._address}' must consist of four octets, separated by dots.`;
      return false;
    }

    for (let i = 0; i < 4; i++) {
      let quad = parseInt(octet[i], 10);

      if (!quad && quad !== 0) {
        this._error = `Each octet in the IP address '${this._address}' must be a number.`;
        return false;
      } else if (quad < 0) {
        this._error = `Octet values in the IP address '${this._address}' cannot be negative.`;
        return false;
      } else if (255 < quad) {
        this._error = `Octet values in the IP address '${this._address}' cannot exceed 255.`;
        return false;
      }
    }

    return true;
  }

  /**
   * Validate the IP address.
   * @returns {boolean} True if the address is valid, false otherwise.
   */
  isValidAddress() {
    if (!isNaN(this._validAddress)) return !!this._validAddress;

    if (this.isValidIPv4()) return (this._validAddress = true);
    if (this.isValidIPv6()) return (this._validAddress = true);

    if (!this.isValidIPv4() && !this.isValidIPv6()) {
      this._error = `The provided address '${this._address}' is not a valid IP address.`;
    }

    return false;
  }

  /**
   * Validate the IP address.
   * @returns {boolean} True if valid, false otherwise.
   */
  isValid() {
    if (!isNaN(this._valid)) return !!this._valid;

    if (this.isValidCIDR() && this.isValidAddress())
      return (this._valid = true);

    return (this._valid = false);
  }
}

/**
 * Calculates the required subnet size to accommodate a specified number of subnets,
 * given an initial subnet size. The result may be fractional if the number of subnets
 * does not align with bit boundaries (e.g., accommodating 257 addresses).
 *
 * @param {number} subnetCount The number of subnets to be accommodated.
 * @param {number} initialSubnetSize The size of the initial subnet.
 * @returns {number} The calculated subnet size required to accommodate the specified number of subnets.
 * @throws {Error} Throws an error if input parameters are not positive numbers.
 */
export function calculateRequiredSubnetSize(subnetCount, initialSubnetSize) {
  if (
    typeof subnetCount !== "number" ||
    subnetCount <= 0 ||
    typeof initialSubnetSize !== "number" ||
    initialSubnetSize <= 0
  ) {
    throw new Error(
      "Both subnetCount and initialSubnetSize must be positive numbers.",
    );
  }

  const logBase2 = Math.log2 || ((n) => Math.log(n) / Math.log(2));
  return initialSubnetSize - logBase2(subnetCount);
}
