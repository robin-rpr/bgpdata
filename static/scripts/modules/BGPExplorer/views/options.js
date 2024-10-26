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
import Mustache from "mustache";

const template = `
<div class="options__download" style="border: 0px solid #C0C0C0; padding: 10px 5px 5px 10px; margin-bottom: 5px;">
    <input class="download__button" type="button" class="getsvg" value="Download SVG" style="width:100%; color: white;border:0;padding:8px 10px 8px 10px;border-radius:3px;" />
</div>
<div class="options__animation-speed" style="border:0px solid #C0C0C0; padding: 0px 5px 5px 10px;margin-bottom:5px;">
    <label class="animation-speed__label" for="animation-speed__slider">Animation speed:</label>
    <input class="animation-speed__input" type="text" value="1" style="border: 0; color: #f6931f; background:transparent; width:15px;" />
    <div class="animation-speed__slider" style="width:100%;"></div>
</div>
<div class="options__graph-depth" style="border: 0px solid #C0C0C0; padding: 0px 5px 5px 10px; margin-bottom: 5px;">
    <label class="graph-depth__slider" for="graph-depth__input">Hide AS if path longer than:</label>
    <input class="graph-depth__input" type="text" value="1" style="border: 0; color: #f6931f; background:transparent; width:30px;" />
    <div class="graph-depth__slider" style="width:100%;"></div>
</div>
<div class="options__graph-link-weight" style="border: 0px solid #C0C0C0; padding: 0px 5px 5px 10px; margin-bottom: 5px;">
    <label class="graph-link-weight__slider" for="graph-link-weight__input">Show AS links if # RIS peers:</label>
    <input class="graph-link-weight__input" type="text" value="1" style="border: 0; color: #f6931f; background: transparent; width: 30px;" />
    <div class="graph-link-weight__slider" style="width:100%;"></div>
</div>
<div class="options__graph-restore" style="border: 1px solid #C0C0C0; padding: 0px 5px 5px 10px; margin-bottom: 5px;">
    <label class="graph-restore__label" for="graph-restore__input">Restore graph:</label>
    <input class="graph-restore__input" type="button" value="Restore" style="width: 80px;" />
</div>
`;

class OptionsView {
  constructor(self) {
    this._self = self;

    this.element = jQuery("<div>")
      .addClass("option")
      .append(Mustache.render(template, this));

    this.downloadButtonElement = this.element.find(".download__button");
    this.graphDepthInputElement = this.element.find(".graph-depth__input");
    this.graphDepthSliderElement = this.element.find(".graph-depth__slider");
    this.graphRestoreInputElement = this.element.find(".graph-restore__input");
    this.graphLinkWeightInputElement = this.element.find(
      ".graph-link-weight__input",
    );
    this.graphLinkWeightSliderElement = this.element.find(
      ".graph-link-weight__slider",
    );
    this.animationSpeedInputElement = this.element.find(
      ".graph-link-weight__input",
    );
    this.animationSpeedSliderElement = this.element.find(
      ".graph-link-weight__slider",
    );
  }

  _initDOMListeners() {
    this.downloadButtonElement.on("click", () => {
      this._screenshot();
    });
  }

  /**
   * This method draws this module (eg. inject the DOM and elements).
   */
  render() {
    this.graphDepthInputElement.val(this._self.graphView.maxHops);

    this.graphDepthSliderElement.slider({
      orientation: "horizontal",
      range: "min",
      min: 1,
      max: this._self.graphView.maxHops,
      value: this._self.graphView.maxHops,
      slide(event, slider) {
        this.graphDepthInputElement.val(slider.value);
        this._self.messages.dispatch("maxhops:changed", slider.value);
      },
    });

    this.graphLinkWeightInputElement.val(this._self.graphView.minLinks + 1);

    this.graphLinkWeightSliderElement.slider({
      orientation: "horizontal",
      range: "min",
      min: 1,
      max: 100,
      value: this._self.graphView.minLinks + 1,
      slide(event, ui) {
        this.graphLinkWeightInputElement.val(ui.value);
        this._self.messages.dispatch("minlinks:changed", ui.value - 1);
      },
    });

    this.animationSpeedInputElement.val(this._self.animationSpeed);

    this.animationSpeedSliderElement.slider({
      orientation: "horizontal",
      range: "min",
      min: 1,
      max: 10,
      value: Number(this._self.animationSpeed),
      slide: (event, ui) => {
        this.animationSpeedInputElement.val(ui.value);
        this._self.messages.dispatch("animationSpeedChanged", ui.value);
      },
    });
  }

  _screenshot() {
    const content = this._self.element.find(".graph__container");

    const clone = content.clone();

    let svg = clone.find("svg");

    svg.removeAttr("height").removeAttr("width");

    const x =
      this._self.graphView.graph.getMinX(true) -
      this._self.config.graph.nodeWidth;

    const y =
      this._self.graphView.graph.getMinY(true) -
      this._self.config.graph.nodeHeight;

    const width =
      this._self.graphView.graph.getMaxX(true) -
      x +
      this._self.config.graph.nodeWidth;

    const height =
      this._self.graphView.graph.getMaxY(true) -
      y +
      this._self.config.graph.nodeHeight;

    svg.attr("viewBox", x + " " + y + " " + width + " " + height);

    const preface = '<?xml version="1.0" standalone="no"?>\r\n';
    const blob = new Blob([preface, clone.html()], {
      type: "image/svg+xml;charset=utf-8",
    });

    const url = URL.createObjectURL(blob);
    const ahref = document.createElement("a");

    ahref.href = url;
    ahref.download = "screenshot.svg";
    document.body.appendChild(ahref);
    ahref.click();
    document.body.removeChild(ahref);
  }
}

export default OptionsView;
