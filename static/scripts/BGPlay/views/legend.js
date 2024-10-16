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
