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
