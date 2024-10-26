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
import ngraph from "ngraph.graph";
import forcelayout from "ngraph.forcelayout";

/**
 * Layout Web Worker
 * @description This Web Worker is responsible for calculating the positions of the Nodes in the Graph.
 * @link https://github.com/anvaka/ngraph.forcelayout
 * @param {Object} event
 */
self.onmessage = (event) => {
  const { nodes, edges, iterations, gravity } = event.data;

  // Initialize.
  const graph = ngraph();

  for (const key in nodes) graph.addNode(key);

  for (const key in edges) {
    graph.addLink(
      BigInt(key) & 0xffffffffn, // Source
      (BigInt(key) >> 32n) & 0xffffffffn, // Target
    );
  }

  const layout = forcelayout(graph, { gravity });
  for (let i = 0; i < iterations; i++) layout.step();

  const positions = {};
  for (const key in nodes) positions[key] = layout.getNodePosition(key);

  // Return.
  self.postMessage(positions);
};
