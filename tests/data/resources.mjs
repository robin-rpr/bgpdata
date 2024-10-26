/*** 
BGPDATA - BGP Data Collection and Analytics Service

This software is part of the BGPDATA project, which is designed to collect, process, and analyze BGP data from various sources.
It helps researchers and network operators get insights into their network by providing a scalable and reliable way to collect and process BGP data.

Author: Robin Röper

© 2024 BGPDATA. All rights reserved.

Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:
1. Redistributions of source code must retain the above copyright notice, this list of conditions, and the following disclaimer.
2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions, and the following disclaimer in the documentation and/or other materials provided with the distribution.
3. Neither the name of BGPDATA nor the names of its contributors may be used to endorse or promote products derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
***/
export const IPv4 = {
  correct: [
    "140.78.93.50",
    "103.7.28.3",
    "200.14.36.4",
    "218.248.160.0/20",
    "149.224.96.0/20",
    "132.86.0.0/16",
    "211.188.11.0/24",
    "41.138.32.0/20",
    "140.78.0.0/16",
    "140.78.90.50/32",
    "67.222.8.0/21",
    "66.54.72.0/22",
    "103.7.28.0/9",
    "209.129.0.0/16",
    "164.41.0.0/16",
    "196.201.225.255/24",
    "84.24.0.0/13",
  ],
  incorrect: [
    "120.120.",
    "103.49.34.",
    "120.120./16",
    "103.49.34./16",
    "120.120/16",
    "103.49.34/16",
    "140.78.90.50/33",
    "728.508.867.858",
    "726.213.37.760",
    "829.305.805.93/abc",
    "829.305.805.93/a123",
    "226.49.454.926/202",
    "282.835.851.842/21",
    "566.790.778.577/300",
    "268.363.962.591/120",
    "256.572.189.27/1",
    "179.920.479.74/8",
    "701.3.441.426//",
  ],
};
export const IPv6 = {
  correct: [
    "2001:418:2402::/48",
    "2001:67c:2e8::/48",
    "2001:df0:21e::/48",
    "2620:0:181e::/48",
    "2001:c08::/32",
    "2002:c000:0204::/48",
    "1::1/128",
  ],
  incorrect: [
    "45:xi:op:8r:1a:4n:tp:xa::",
    "uk:xk:dd:lc:wm:ze:un:js::",
    "fj:wk:kf:th:59:oe:zt:vb::",
    "7n:fd:zj:0y:cr:89:b8:pw::",
    "8q:k7:6n:mn:mx:ug:vr:uq::",
    "7k:7v:et:05:mq:5m:no:m9::/a123",
    "eo:19:e7:71:if:hc:7y:lg::/abc",
    "x3:cg:47:tp:tb:fq:cb:lt::/48",
    "e1:hk:vo:j4:9n:ww:t4:is::/292",
    "in:6u:ic:uy:1y:z4:ph:fd:://",
  ],
};
export const Hostnames = {
  correct: [
    "ripe.net",
    "arin.net",
    "apnic.net",
    "ieee.org",
    "ietf.org",
    "internetsociety.org",
    "archive.org",
  ],
  incorrect: [
    ".dkzaoyfkS",
    "ruakyrufgn?",
    "ahfwbwsmX",
    "mtcc/ofdidpw",
    "vvjvyp%yG",
    "azg$knzrd[",
    "fjhyyj\"kml'",
    "gb+iszwlk",
    "zgmwp*refkZ",
    "ymfagbkep8!",
  ],
};
export const ASNs = {
  correct: [
    "AS1205",
    "AS1239",
    "AS48539",
    "AS3491",
    "AS1785",
    "AS2914",
    "AS12049",
    "AS39201",
    "AS65540",
    "AS0.39201",
    "AS1.4",
  ],
  incorrect: [
    "-1",
    "AS-1",
    "ASnull",
    "ASundefined",
    "13424127909",
    "10945783486",
    "6605266539",
    "6698760244",
    "7428227710",
    "8481101665",
    "9761739245",
    "4557007076",
    "14242769664",
    "9295652116",
  ],
};

export const Countries = {
  correct: [
    "AF",
    "AX",
    "AL",
    "DZ",
    "AS",
    "AD",
    "AO",
    "AI",
    "AQ",
    "AG",
    "AR",
    "AM",
    "AW",
    "AU",
    "AT",
    "AZ",
    "BS",
  ],
  incorrect: ["12", "SO", "HX", "LM", "LL", "DXS", "ABCD", "12345"],
};
