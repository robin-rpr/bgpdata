import ControlsView from "./views/controls.js";
// import ControllerQueryView from "./views/controllerQuery.js";
import GraphView from "./views/graph.js";
import InfoPanelView from "./views/infoPanel.js";
import LegendView from "./views/legend.js";
import OptionsView from "./views/options.js";
import TimelineView from "./views/timeline.js";
import { UnionFind } from "reunionjs";

import MessageAggregator from "./model/message-aggregator.js";
import config from "./config.js";

/**
 * BGPlay
 * @link https://stat.ripe.net
 * @license LICENSE.md - Usage subject to the stated conditions.
 */
class BGPlay {
  config = config;
  messages = new MessageAggregator();

  _instant = null;

  constructor(element, data, params) {
    this.resource = params.resource || null;
    this.rrcs = params.rrcs || [];
    this.rrcPeers = params.rrcPeers || [];
    this.modes = params.modes || [];
    this.useStreaming = params.useStreaming || false;
    this.ignoreReannouncements = params.ignoreReannouncements || true;
    this.showAnimationControls = params.showAnimationControls || true;
    this.showResourceController = params.showResourceController || true;
    this.doRepeatLastEvent = params.doRepeatLastEvent || true;
    this.animationSpeed = Number(params.animationSpeed) || 1;

    this.starttime = new Date(data.query_starttime).getUTCDate();
    this.endtime = new Date(data.query_endtime).getUTCDate();

    this.element = typeof element === "object" ? element : jQuery(element);
    this.data = data;

    this._instant = this.starttime;

    // Initialize Streaming-mode if enabled.
    if (this.useStreaming) {
      this._initStreaming();
    }

    this.start = performance.now();

    this.edges = {};
    this.nodes = Object.fromEntries(
      data.nodes.map((node) => [node.as_number, node]),
    );

    this.sources = data.sources;
    this.targets = data.targets;
    this.state = data.initial_state;
    this.events = data.events;

    this.paths = new Set();
    this.dynamic = new Set();
    this.initial = new Map();
    this.withdrawn = new Set();

    let index = BigInt(0);
    let indices = new Map();
    let subtrees = new Map();
    let uf = new UnionFind();

    // Initialization.
    for (const { source_id, path, target_prefix } of data.initial_state) {
      this.paths.add(source_id + "!" + path.join(",") + "!" + target_prefix);
      this.initial.set(source_id + "!" + target_prefix, path.join(","));
    }

    const stop = this.paths.size;

    // Processing Initial Events.
    for (const { attrs, type } of data.events) {
      // Handle withdrawals.
      if (type === "W") {
        this.withdrawn.add(attrs.source_id + "!" + attrs.target_prefix);
        continue;
      }

      // Negate withdrawals on re-announcements.
      if (this.withdrawn.has(attrs.source_id + "!" + attrs.target_prefix)) {
        this.withdrawn.delete(attrs.source_id + "!" + attrs.target_prefix);
      }

      // Create a unique key for the path.
      /*const key =
        attrs.source_id +
        "!" +
        attrs.path.join(",") +
        "!" +
        attrs.target_prefix;*/

      // If the path was in the initial state and has an event, it's dynamic.
      if (this.initial.has(attrs.source_id + "!" + attrs.target_prefix)) {
        // Get the initial path, if it exists, if not, it's undefined.
        const path = this.initial.get(
          attrs.source_id + "!" + attrs.target_prefix,
        );

        // Let's see if the path has changed from the initial path.
        if (attrs.path.join(",") !== path && path !== undefined) {
          // Path has changed from the initial path; mark as dynamic.
          this.dynamic.add(
            attrs.source_id + "!" + path + "!" + attrs.target_prefix,
          );
        }
      }

      // Add new paths to the set of all paths.
      //this.paths.add(key);
    }

    this.static = this.paths.difference(this.dynamic);

    console.debug("Static Paths are:", this.static);
    console.debug("Dynamic Paths are:", this.dynamic);

    // Initialize variables
    const sets = []; // Each set is { uf: UnionFind, paths: [] }

    // Process each AS-path
    for (const key of this.paths) {
        const [_, pathStr] = key.split("!");
        const path = pathStr.split(",").filter((v, i, a) => i === 0 || v !== a[i - 1]);
    
        let addedToSet = false;
        for (const set of sets) {
            const { uf } = set;
            let hasCycle = false;
            const tempUnions = [];
    
            for (let i = 0; i < path.length - 2; i++) {
                const u = path[i];
                const v = path[i + 1];
                if (uf.find(u) === uf.find(v)) {
                    hasCycle = true;
                    break;
                } else {
                    tempUnions.push([u, v]);
                }
            }
    
            if (!hasCycle) {
                tempUnions.forEach(([u, v]) => uf.union(u, v));
                set.paths.push(key);
                addedToSet = true;
                break;
            }
        }
    
        if (!addedToSet) {
            const uf = new UnionFind();
            for (let i = 0; i < path.length - 2; i++) {
                const u = path[i];
                const v = path[i + 1];
                uf.union(u, v);
            }
            sets.push({ uf, paths: [key] });
        }
    }

    console.debug("Sets are:", sets);
  
    // Now, 'sets' contains the partitioned paths as per the algorithm
    // Each set in 'sets' represents a tree (acyclic graph)

    // For visualization, assign a subtree ID to each set
    for (let i = 0; i < sets.length; i++) {
      const set = sets[i];
      const subtreeID = i; // Assign a unique ID to each set
      for (const key of set.paths) {
        subtrees.set(key, subtreeID);
      }
    }

    let i = 0;
    for (const key of this.paths) {
      let [source_id, path, target_prefix] = key.split("!");

      path = path
        .split(",")
        .reduce(
          (acc, node) => (acc[acc.length - 1] !== node && acc.push(node), acc),
          [],
        );

      const size = path.length;

      for (let j = 0; j < size - 1; j++) {
        const low = Math.min(path[j], path[j + 1]);
        const high = Math.max(path[j], path[j + 1]);
        const packed = (BigInt(high) << 32n) | BigInt(low);
        const dynamic = this.dynamic.has(key);
        //const subtree = dynamic ? null : getSubtree(source_id, target_prefix);
        const subtree = dynamic ? null : subtrees.get(key);
        //console.debug("Subtree for", key, "is", subtree);

        (this.edges[packed] = this.edges[packed] || {})[
          subtree != null ? subtree : target_prefix
        ] = {
          source_id,
          target_prefix,
          path,
          subtree,
          dynamic,
          last: false,
          drawn: i < stop,
        };
      }
      i++;
    }

    console.debug(subtrees);

    // this.infoPanelView = new InfoPanelView(this);
    //this.controlsView = new ControlsView(this);
    this.graphView = new GraphView(this);
    const end = performance.now();
    console.debug("Took", end - this.start, "ms");
    //this.legendView = new LegendView(this);
    // this.timelineView = new TimelineView(this);
    //this.optionsView = new OptionsView(this);
  }

  set instant(value) {
    this._instant = value;
    this.messages.dispatch("instant");
  }

  get instant() {
    return this._instant;
  }

  render() {
    // this.infoPanelView.render();
    //this.controlsView.render();
    this.graphView.render();
    //this.legendView.render();
    // this.timelineView.render();
    //this.optionsView.render();
  }

  _initStreaming() {
    this.streamingFacade = new StreamingFacade(this);
    this.streamingAdapter = new StreamingAdapter(this);

    this.streamingFacade.connect({
      onEvent: this.streamingAdapter.addNewEvent,
      onConnect: () => {
        this.streamingFacade.subscribe(params);
      },
    });
  }
}

export default BGPlay;
