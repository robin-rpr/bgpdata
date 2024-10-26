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
export default {
  safetyMaximumNodes: 160,
  safetyMaximumEvents: 30000,
  possibleRrcs: [
    0, 1, 2, 3, 4, 5, 6, 7, 10, 11, 12, 13, 14, 15, 16, 18, 20, 21, 22, 23, 24,
    25, 26,
  ],
  selectedRrcsPrefix: [0, 6, 13, 16, 19, 23, 26],
  selectedRrcsAS: [0, 6, 13, 16, 19, 23, 26],
  selectableRrcs: true,
  rrcLocations: {
    0: "Amsterdam",
    1: "London",
    2: "Paris",
    3: "Amsterdam",
    4: "Geneva",
    5: "Vienna",
    6: "Otemachi",
    7: "Stockholm",
    10: "Milan",
    11: "New York",
    12: "Frankfurt",
    13: "Moscow",
    14: "Palo Alto",
    15: "Sao Paulo",
    16: "Miami",
    18: "Barcelona",
    19: "Johannesburg",
    20: "Zurich",
    21: "Paris",
    22: "Bucharest",
    23: "Singapore",
    24: "Montevideo",
    25: "Amsterdam",
    26: "Dubai",
  },
  ignoreReannouncementsByDefault: true,
  cumulativeAnimations: true,
  doubleClickTimeInterval: 400,

  controller: {
    parametersInUrl: true,
    disableNotSelectedInstants: true,
  },

  graph: {
    computeNodesPosition: true,
    springEmbedderCycles: 100,
    paperMinHeight: 360,
    whenCoulombRepulsionStarts: 0,
    whenCoulombRepulsionEnds: 95,
    whenHookInteractionsStarts: 35,
    whenHookInteractionsEnds: 95,
    whenOnlyLeavesRepulsion: 95,
    whenEdgeNodeRepulsionStarts: 85,
    pathBold: 6,
    maxHops: Infinity,
    minLinks: 0,
    pruneByPeer: "all",
    defaultDeepForASesEsploration: 2,
    // Each number below indicates the amount of milliseconds that a blink lasts expressed as an alternation of normal and the thick states.
    // If you want to increase the number of blinks, add new delays in the array.
    animationPathWithdrawalDelays: [300, 300, 300, 300, 300, 300],
    animationMinorChangesDelays: [300, 300],
    animationPathInsertionDelay: 800,
    animationPathChangeDelay: 1200,
    pathColors: [
      "#ff6f6f",
      "#ffc266",
      "#80ffa6",
      "#6c94bf",
      "#d085ff",
      "#853333",
      "#9e8533",
      "#c6ffe6",
      "#d9ebff",
      "#853d70",
      "#b58c80",
      "#8c8c66",
      "#1a7f5f",
      "#333366",
      "#b3008f",
      "#a60066",
      "#8c9900",
      "#00ffee",
      "#0073e6",
      "#ff3399",
      "#ff8c66",
      "#003d3d",
      "#0073e6",
      "#b3597c",
      "#ffc6b3",
      "#c6ff33",
      "#008080",
      "#99b3ff",
      "#ff4d4d",
      "#ff8533",
      "#4d4d00",
      "#00b3b3",
      "#b3c6cc",
      "#734d4d",
      "#332b26",
      "#dfff80",
      "#00b3b3",
      "#ff4d80",
      "#853d00",
      "#23698c",
      "#330d17",
      "#ffcc66",
      "#53a674",
      "#b3c6ff",
      "#993d71",
      "#ff6666",
      "#e6b3b3",
    ],
    pathDefaultStrokeDasharray: [""],
    pathStaticStrokeDasharray: ["- "],
    pathIncrementalColoringForTwoPrefixes: true,
    pathColorsDoublePrefixOne: 0xff0000,
    pathColorsDoublePrefixTwo: 0x1c7d01,
    pathWeight: 1.5,
    notSelectedElementOpacity: 0.15,
    pathInterline: 0.8,
    patInterlineDistributed: true,
    arcsBeamMaxWidth: true,
    staticDeviation: false,
    nodeWidth: 35,
    nodeHeight: 25,
    nodeMouseoverMilliseconds: 600,
    pathMouseoverMilliseconds: 200,
    nodeSelectChildrenMilliseconds: 1000,

    nodeTextColor: "#000000",
    sourceTextColor: "#4CB4E7",
    targetTextColor: "white",

    nodeBorderColor: "#000000",
    sourceBorderColor: "#4CB4E7",
    targetBorderColor: "#FF3B3F",

    nodeColor: "white",
    targetColor: "#FF3B3F",
    sourceColor: "white",

    nodeTextFontSize: "11px", // Any valid CSS Font Size value.

    hideDefaultRoutes: true,
    strUpdateTimer: 1000, // Milliseconds.
  }
};
