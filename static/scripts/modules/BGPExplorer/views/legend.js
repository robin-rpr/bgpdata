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
import Mustache from "mustache";

const template = `
<div>
    <div class="bgplayLegendItem">
        <div style="color: #FFFFFF; background: #FF3B3F; width: 110px; height: 20px; border: 1px solid #FF3B3F; border-radius: 0px;">
            Origin AS
        </div>
    </div>

    <div class="bgplayLegendItem">
        <div style="color: #4CB4E7; background: #FFFFFF; width: 110px; height: 20px; border: 1px solid #4CB4E7; border-radius: 0px;">
            Collector peer
        </div>
    </div>

    <div class="bgplayLegendItem">
        <div style="color: #000000; background: #FFFFFF; width: 110px; height: 20px; border: 1px solid #000000; border-radius: 0px;">
            Other
        </div>
    </div>

    <div class="bgplayLegendItem">
        <div style="width: 110px; height: 15px; border-top: 2px solid #000000; margin-top: 2px;">
            Dynamic path
        </div>
    </div>

    <div class="bgplayLegendItem">
        <div style="width: 110px; height: 15px; border-top: 2px dashed #000000; margin-top: 2px;">
            Static path
        </div>
    </div>
</div>
`;

class LegendView {
  /**
   * Initializes a new instance of the LegendView class.
   *
   * @param {Object} options A map of initialization parameters.
   */
  constructor(self) {
    this._self = self;

    this.element = jQuery("<div>")
      .addClass("legend")
      .append(Mustache.render(template, this));
  }

  /**
   * This method draws this module (eg. inject the DOM and elements).
   */
  render() {
    this._self.element.append(this.element);
  }
}

export default LegendView;
