import Mustache from "mustache";
import PublicSansRegular from "../../../assets/fonts/PublicSans-Regular.ttf";
import PublicSansBold from "../../../assets/fonts/PublicSans-Bold.ttf";
import LayoutWorker from "../workers/layout.worker.js";
import { Cull } from "@pixi-essentials/cull";
import { Viewport } from "pixi-viewport";
import * as PIXI from "pixi.js";
import { translate } from "../utils/graph.js";

const template = `
<input type="button" class="graph__zoom graph__zoom--in" value="+"/>
<input type="button" class="graph__zoom graph__zoom--out" value="-"/>
<div class="graph__container"></div>
`;

class GraphView {
  animating = false;
  dragging = null;
  hovering = null;
  selected = null;
  focused = null;
  nodes = null;

  nodeRadius = 35;
  nodeBorderWidth = 2;
  nodeHitWidth = 5;
  nodeHoverBorderColor = 0x000000;

  constructor(self) {
    this._self = self;

    PIXI.Cull = Cull;
    PIXI.Viewport = Viewport;

    this.element = jQuery("<div>")
      .addClass("graph")
      .css({ width: "800px", height: "600px" })
      .append(Mustache.render(template, this));

    this.width = this.element.width();
    this.height = this.element.height();
    this.resolution = window.devicePixelRatio;

    this.graphContainer = this.element.find(".graph__container");
    this.graphZoomInElement = this.element.find(".graph__zoom--in");
    this.graphZoomOutElement = this.element.find(".graph__zoom--out");

    /**
     * PIXI.js
     * @description A JavaScript library for creating rich, interactive graphics in web browsers. PIXI.js leverages WebGL for high-performance visualizations and falls back to HTML5's canvas if needed. It's ideal for building 2D gaming applications, interactive graphics, and data visualizations.
     * @see Documentation https://pixijs.com
     */
    this.app = new PIXI.Application({
      width: this.width,
      height: this.height,
      resolution: this.resolution,
      backgroundAlpha: 0, // Make background transparent.
      antialias: true, // Enable anti-aliasing.
      autoDensity: true,
      autoStart: false, // Disable automatic rendering by ticker, render manually instead, only when needed.
    });

    /**
     * PIXI.js @pixi-essentials/cull
     * @description This is eliminating the rendering of polygons or faces of a 2D object that are not visible to the camera.
     */
    this.cull = new PIXI.Cull();

    this._initMessageListeners();
    this._initDOMListeners();
  }

  _initMessageListeners() {
    this._self.messages.on("instant", (value) => {
      console.debug("Instant changed to", value);
    });

    this._self.messages.on("maxhops", (value) => {
      console.debug("Maxhops changed to", value);
    });

    this._self.messages.on("minlinks", (value) => {
      console.debug("Minlinks changed to", value);
    });

    this._self.messages.on("rrcpeers", (value) => {
      console.debug("Rrcpeers changed to", value);
    });
  }

  _initDOMListeners() {
    this.graphZoomInElement.on("click", () => {
      this.zoomIn();
    });

    this.graphZoomOutElement.on("click", () => {
      this.zoomOut();
    });
  }

  /**
   * This method draws this module (eg. inject the DOM and elements).
   */
  async render() {
    this._self.element.append(this.element);
    this.graphContainer.append(this.app.view);

    this.nodes = Object.values(this._self.nodes);
    for (const key in this._self.edges) {
      const max = 14; // Maximum total width for all parallel arcs.
      
      let keys = [];
      for (const id in this._self.edges[key]) {
        if (this._self.edges[key][id].drawn) keys.push(id);
        if (keys.length == max * 2) break;
      }

      const spacing = max / keys.length;

      let index = 0, last = null;
      for (const id of keys) {
        // Calculate and assign new position.
        const offset = -max / 2 + spacing / 2 + index++ * spacing;
        this._self.edges[key][id].position = offset;
        last = id;
      }
      if (last) this._self.edges[key][last].last = true;
    }

    // Compute layout in Worker.
    const positions = await new Promise((resolve) => {
      const worker = new LayoutWorker();

      worker.onmessage = (event) => {
        resolve(event.data);
        worker.terminate();
      };

      worker.postMessage({
        nodes: this._self.nodes,
        edges: this._self.edges,
        iterations: 400,
        gravity: -800,
      });
    });

    for (const key in this._self.nodes) {
      const position = positions[key];
      this._self.nodes[key].x = position.x;
      this._self.nodes[key].y = position.y;
    }    

    // Normalize Node Positions.
    let minNodeX = Infinity,
      maxNodeX = -Infinity,
      minNodeY = Infinity,
      maxNodeY = -Infinity;

    for (let node of this.nodes) {
      minNodeX = Math.min(minNodeX, node.x);
      maxNodeX = Math.max(maxNodeX, node.x);
      minNodeY = Math.min(minNodeY, node.y);
      maxNodeY = Math.max(maxNodeY, node.y);
    }

    const graphWidth = Math.abs(maxNodeX - minNodeX);
    const graphHeight = Math.abs(maxNodeY - minNodeY);
    this.worldWidth = Math.max(this.width * 2, graphWidth * 1.1);
    this.worldHeight = Math.max(this.height * 2, graphHeight * 1.1);

    for (const node of this.nodes) {
      node.x = node.x - minNodeX - graphWidth / 2 + this.worldWidth / 2;
      node.y = node.y - minNodeY - graphHeight / 2 + this.worldHeight / 2;
    }

    const color = (value) => 
      [
          0x1f77b4, // Blue
          0xff7f0e, // Orange
          0x2ca02c, // Green
          0xd62728, // Red
          0x9467bd, // Purple
          0x8c564b, // Brown
          0xe377c2, // Pink
          0x7f7f7f, // Gray
          0xbcbd22, // Yellow-green
          0x17becf, // Cyan
          0x9edae5, // Light cyan
          0xffc300, // Yellow
          0xff5733  // Coral
      ][value % 13];

    // Create Viewport.
    this.viewport = new PIXI.Viewport({
      screenWidth: this.width,
      screenHeight: this.height,
      worldWidth: this.worldWidth,
      worldHeight: this.worldHeight,
      passiveWheel: false,
      divWheel: this._self.element,
      events: this.app.renderer.events,
    });

    // Preload fonts.
    PIXI.Assets.addBundle("fonts", {
      "Public Sans Regular": {src: PublicSansRegular, data: { family: "Public Sans Regular" }},
      "Public Sans Bold": {src: PublicSansBold, data: { family: "Public Sans Bold" }},
    });
    await PIXI.Assets.loadBundle("fonts");

    this.app.stage.addChild(this.viewport);
    this.app.stage.eventMode = 'static';
    this.app.stage.hitArea = this.app.screen;
    this.viewport
      .drag()
      .pinch()
      .wheel()
      .decelerate()
      .clampZoom({ minWidth: this.width, minHeight: this.height });

    // Create layers.
    const linksLayer = new PIXI.Container();
    this.viewport.addChild(linksLayer);
    const frontLinksLayer = new PIXI.Container();
    this.viewport.addChild(frontLinksLayer);
    const nodesLayer = new PIXI.Container();
    this.viewport.addChild(nodesLayer);
    const labelsLayer = new PIXI.Container();
    this.viewport.addChild(labelsLayer);
    const frontNodesLayer = new PIXI.Container();
    this.viewport.addChild(frontNodesLayer);
    const frontLabelsLayer = new PIXI.Container();
    this.viewport.addChild(frontLabelsLayer);

    // Create textures.
    const circleGraphics = new PIXI.Graphics();
    circleGraphics.beginFill(0xffffff);
    circleGraphics.drawCircle(this.nodeRadius, this.nodeRadius, this.nodeRadius);
    const circleTexture = this.app.renderer.generateTexture(
      circleGraphics,
      PIXI.settings.SCALE_MODE,
      this.resolution,
    );

    const circleBorderGraphics = new PIXI.Graphics();
    circleBorderGraphics.lineStyle(this.nodeBorderWidth, 0xffffff);
    circleBorderGraphics.drawCircle(
      this.nodeRadius + this.nodeBorderWidth,
      this.nodeRadius + this.nodeBorderWidth,
      this.nodeRadius,
    );

    const circleBorderTexture = this.app.renderer.generateTexture(
      circleBorderGraphics,
      PIXI.settings.SCALE_MODE,
      this.resolution,
    );

    // Create node graphics.
    for (const node of this.nodes) {
      node.gfx = {};

      node.gfx.node = new PIXI.Container();
      node.gfx.node.x = node.x;
      node.gfx.node.y = node.y;
      node.gfx.node.eventMode = "static";
      node.gfx.node.cursor = "pointer";
      node.gfx.node.hitArea = new PIXI.Circle(0, 0, this.nodeRadius + this.nodeHitWidth);

      const circle = new PIXI.Sprite(circleTexture);
      circle.name = "CIRCLE";
      circle.x = -circle.width / 2;
      circle.y = -circle.height / 2;
      circle.tint = 0xf2f2f2;
      node.gfx.node.addChild(circle);

      const circleBorder = new PIXI.Sprite(circleBorderTexture);
      circleBorder.name = "CIRCLE_BORDER";
      circleBorder.x = -circleBorder.width / 2;
      circleBorder.y = -circleBorder.height / 2;
      circleBorder.tint = 0x3b75af;
      node.gfx.node.addChild(circleBorder);

      const name = new PIXI.Text(node.as_number, {
        fontFamily: "Public Sans Bold",
        fontSize: 16,
        fill: 0x333333,
        align: "center",
      });

      name.name = "NAME";
      name.x = -name.width / 2;
      name.y = -name.height / 2;
      node.gfx.node.addChild(name);

      node.gfx.label = new PIXI.Container();
      node.gfx.label.renderable = false;
      node.gfx.label.x = node.x;
      node.gfx.label.y = node.y;

      const label = new PIXI.Text(node.owner, {
        fontFamily: "Public Sans Regular",
        fontSize: 23,
        fill: 0x333333,
        align: "center",
      });

      const padding = 4;
      label.name = "LABEL";
      label.x = -label.width / 2;
      label.y = this.nodeRadius + this.nodeHitWidth + padding;

      const labelBackground = new PIXI.Sprite(PIXI.Texture.WHITE);
      labelBackground.name = "LABEL_BACKGROUND";
      labelBackground.x = -(label.width + padding * 2) / 2;
      labelBackground.y = this.nodeRadius + this.nodeHitWidth;
      labelBackground.width = label.width + padding * 2;
      labelBackground.height = label.height + padding * 2;
      labelBackground.tint = 0xffffff;
      labelBackground.alpha = 0.5;

      node.gfx.label.addChild(labelBackground);
      node.gfx.label.addChild(label);

      // Events.
      const translate = (event) => {
        const point = this.viewport.toWorld(event.global)
        node.x = point.x;
        node.y = point.y;
        this.update();
      }
      const pointerdown = () => {
        this.dragging = node.gfx.node;
        this.viewport.pause = true;
        this.app.stage.on("pointermove", translate);
        
      };
      const pointerup = () => {
        if (this.dragging) {
          this.dragging = null;
          this.viewport.pause = false;
          this.app.stage.off("pointermove", translate);
        }
      };
      node.gfx.node.on("pointerup", () => pointerup());
      node.gfx.node.on("pointerdown", () => pointerdown());
      this.app.stage.on('pointerup', () => pointerup());
      this.app.stage.on('pointerupoutside', () => pointerup());
      node.gfx.node.on("pointerupoutside", () => {
        if (this.dragging) {
          this.dragging = null;
          this.viewport.pause = false;
          this.app.stage.off("pointermove", translate);
        }
      });
      node.gfx.node.on("mouseenter", () => {
        circle.tint = 0xff0000;
        circleBorder.tint = 0x000000;
        node.gfx.label.renderable = true;
        this.animate();
      })
      node.gfx.node.on("mouseleave", () => {
        circle.tint = 0xf2f2f2;
        circleBorder.tint = 0x3b75af;
        node.gfx.label.renderable = false;
        this.animate();
      })

      // Culling.
      this.cull.add(node.gfx.node);
      this.cull.add(node.gfx.label);

      nodesLayer.addChild(node.gfx.node);
      labelsLayer.addChild(node.gfx.label);
    }

    // Create Link Graphics.
    for (const key in this._self.edges) {
      const source = BigInt(key) & 0xffffffffn;
      const target = (BigInt(key) >> 32n) & 0xffffffffn;

      for (let arc in this._self.edges[key]) {
        arc = this._self.edges[key][arc];
        if (!arc.drawn) continue;

        const sourceNode = this._self.nodes[source];
        const targetNode = this._self.nodes[target];

        const lineLength = Math.max(
          Math.sqrt(
            (targetNode.x - sourceNode.x) ** 2 +
              (targetNode.y - sourceNode.y) ** 2,
          ) -
            this.nodeRadius +
            this.nodeBorderWidth * 2,
          0,
        );
        const lineSize = 2;

        const gfx = new PIXI.Container();
        gfx.x = sourceNode.x;
        gfx.y = sourceNode.y;
        gfx.pivot.set(0, lineSize / 2);
        gfx.rotation = Math.atan2(
          targetNode.y - sourceNode.y,
          targetNode.x - sourceNode.x,
        );
        gfx.eventMode = "static";
        gfx.cursor = "pointer";
        gfx.hitArea = new PIXI.Rectangle(this.nodeRadius + this.nodeBorderWidth, -20 / 2, lineLength, 20);
        let timeout;
        gfx.on("mouseenter", () => {
          timeout = setTimeout(() => {
            this.focused = arc.subtree || arc.source_id+arc.target_prefix;
            this.update();
          }, 400)
        })
        gfx.on("mouseleave", () => {
          clearTimeout(timeout);
          this.focused = null;
          this.update();
        })

        const line = new PIXI.Sprite(PIXI.Texture.WHITE);
        line.name = "LINE";
        line.x = 0;
        line.y = -lineSize / 2;
        line.width = lineLength;
        line.height = lineSize;
        line.tint = arc.dynamic ? 0x000000 : color(arc.subtree);
        
        gfx.addChild(line);

        linksLayer.addChild(gfx);

        this.cull.add(gfx);
        arc.gfx = gfx;

        if (arc.last) break;
      }
    }

    // Initial draw.
    this.reset();
    this.update();

    this.viewport.on("frame-end", () => {
      if (this.viewport.dirty) {
        this.cull.cull(this.app.renderer.screen);

        this.animate();
        this.viewport.dirty = false;
      }
    });
  }

  animate() {
    if (this.animating) return; // Early exit.
    this.animating = window.requestAnimationFrame(() => {
      this.app.render();
      this.animating = false;
    });
  }

  update() {
    for (const node of this.nodes) {
      node.gfx.node.x = node.x;
      node.gfx.node.y = node.y;
      node.gfx.label.x = node.x;
      node.gfx.label.y = node.y;
    }

    for (const key in this._self.edges) {
      const source = BigInt(key) & 0xffffffffn;
      const target = (BigInt(key) >> 32n) & 0xffffffffn;

      for (let arc in this._self.edges[key]) {
        arc = this._self.edges[key][arc];
        if (!arc.drawn) continue;

        const sourceNode = this._self.nodes[source];
        const targetNode = this._self.nodes[target];

        const line = arc.gfx.getChildAt(0);

        // Proceed with drawing the edge only if arc.drawn is true
        const targetDistanceUnitLength = 3; // This value should be adjusted based on your graph's scaling
        // Calculate translation for the current edge
        const translation = translate(
          arc.position * targetDistanceUnitLength,
          sourceNode,
          targetNode,
        );

        // Adjust linkGfx position based on the calculated translation
        arc.gfx.x = sourceNode.x + translation.dx;
        arc.gfx.y = sourceNode.y + translation.dy;

        // Calculate and apply the rotation considering the translation
        arc.gfx.rotation = Math.atan2(
          targetNode.y + translation.dy - (sourceNode.y + translation.dy),
          targetNode.x + translation.dx - (sourceNode.x + translation.dx),
        );

        // Set the length of the line graphic
        const lineLength = Math.max(
          Math.sqrt(
            (targetNode.x - sourceNode.x) ** 2 +
              (targetNode.y - sourceNode.y) ** 2,
          ) -
            this.nodeRadius +
            this.nodeBorderWidth * 2,
          0,
        );

        if(this.focused !== null && (this.focused === arc.subtree || this.focused === arc.source_id+arc.target_prefix)) {
          line.alpha = 1;
          line.height = 4;
          line.y = -4 / 2;
        } else if(this.focused !== null) {
          line.alpha =  0.3; 
          line.height = 2;
          line.y = -2 / 2;
        } else {
          line.alpha =  1; 
          line.height = 2;
          line.y = -2 / 2;
        }

        line.width = lineLength;
        if (arc.last) break;
      }
    }

    this.animate();
  };

  zoomIn() {
    this.viewport.zoom(-this.worldWidth / 10, true);
  }

  zoomOut() {
    this.viewport.zoom(this.worldWidth / 10, true);
  }

  reset() {
    this.viewport.center = new PIXI.Point(
      this.worldWidth / 2,
      this.worldHeight / 2,
    );
    this.viewport.fitWorld(true);
  }
}

export default GraphView;
