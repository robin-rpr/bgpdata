import {
  areMapsEquals,
  arrayContains,
  removeSubArray,
} from "../utils/miscellaneous.js";
import openSlideImage from "../assets/images/openSlide.png";
import closeSlideImage from "../assets/images/closeSlide.png";
import configImage from "../assets/images/config.png";
import streamingOnImage from "../assets/images/bgplay_streaming_on.png";
import streamingOffImage from "../assets/images/bgplay_streaming_off.png";
import deleteImage from "../assets/images/delete.png";
import moreImage from "../assets/images/more.png";
import Mustache from "mustache";

const template = `
<img class="controls__open-button" src="${configImage}" alt="Open config" title="Open config"/>

<div class="controls__animation">
    {{#environment.useStreaming}}
        <div class="animation__streaming-on">
            <img src="${streamingOnImage}" alt="Streaming off" title="Turn streaming off"/> 
        </div>
        <div class="animation__streaming-off"> 
            <img src="${streamingOffImage}" alt="Streaming on" title="Turn streaming on"/> 
        </div>
    {{/environment.useStreaming}}
    <div class="animation__previous">
        <i class="fa-solid fa-backward-step"></i>
    </div>
    <div class="animation__main">
        <div class="main__pause"><i class="fa-solid fa-circle-pause"></i></div>
        <div class="main__start"><i class="fa-solid fa-circle-play"></i></div>
        <div class="main__repeat"><i class="fa-solid fa-rotate-right"></i></div>
    </div>
    <div class="animation__stop">
        <i class="fa-solid fa-stop"></i>
    </div>
    <div class="animation__next">
        <i class="fa-solid fa-forward-step"></i>
    </div>
</div>

<div class="controls__other">
    <div>
        <label for="other__start-time">Start date: </label>
        <input class="other__start-time" type="text"/>
    </div>
    <div>
        <label for="other__end-time">End date: </label>
        <input class="other__end-time" type="text"/>
    </div>
    {{#selectableRrcs}}
        <div class="other__rrc">
            <label class="rrc__label" style="float:left; clear:both; width:100%;margin-bottom:10px;">Route Collectors: </label>
            {{#selectableRrcsObj}}
                <div class="rrc__select">
                    <label class="select__label" for="select__input">{{label}}</label>
                    <input name="select__input" type="checkbox" value="{{id}}" {{#selected}}checked="checked"{{/selected}}/>
                </div>
            {{/selectableRrcsObj}}
        </div>
    {{/selectableRrcs}}
    {{#showResourceController}}
        <div class="other__resource">
            <label for="resource__prefix-label">Resources to reach: </label>
            <div class="resource__prefix-input">
                <div class="resource__prefix-label">
                    {{#prefixes}}
                        <div class="prefix-label__input">
                            <input name="input__add" type="text" value="{{.}}"/>
                            <img class="input__delete" src="${deleteImage}" alt="Delete this prefix" title="Delete this prefix"/>
                        </div>
                    {{/prefixes}}
                    <img class="prefix-label__more" alt="more" title="more" src="${moreImage}"/>
                </div>
            </div>
        </div>
    {{/showResourceController}}
    <div class="other__suppress-reannounce">
        <label for="suppress-reannounce__input">Ignore re-announcements</label>
        <input class="suppress-reannounce__input" type="checkbox" {{#ignoreReannouncements}}checked="checked"{{/ignoreReannouncements}}/>
    </div>
    <div class="other__controls">
        <input type="button" class="controls__save" value="Apply"/>
        <input type="button" class="controls__discard" value="Discard"/>
    </div>
</div>
`;

class ControlsView {
  animation = false;
  slideOpened = false;
  selectableRrcsObj = [];
  releasedPlayButton = true;

  /**
   * The initialization method of this object.
   * @param {Map} options A map of parameters
   */
  constructor(self) {
    this._self = self;

    this.element = jQuery("<div>")
      .addClass("controller")
      .append(Mustache.render(template, this));

    this.possibleRrcs = removeSubArray(
      this._self.config.possibleRrcs,
      this._self.rrcs,
    );

    this.startAnimationInstant = this._self.starttime;
    this.stopAnimationInstant = this._self.endtime;

    for (const rrcId of this._self.config.possibleRrcs) {
      this.selectableRrcsObj.push({
        id: rrcId,
        label: `${rrcId} ${this._self.config.rrcLocations[rrcId]}`,
        selected: arrayContains(this._self.rrcs, rrcId),
      });
    }

    this._self.messages.on("instant:changed", () => {
      let instant = this._self.instant;
      this._self.instant = instant.toString();
    });

    this._self.messages.on("animationEnd", () => {
      this.stop();
    });

    this._self.messages.on("newSample", () => {
      if (this._self.streamingOn) {
        this.element
          .find(".animation__streaming-on")
          .delay(100)
          .fadeTo(100, 0.5)
          .delay(1000)
          .fadeTo(100, 1);
        this.startAnimationInstant = this._self.starttime;
        this.stopAnimationInstant = this._self.endtime;
      }
    });

    this._self.messages.on("animationReload", () => {
      this.reload();
    });

    this._self.messages.on("newSelectionStart", (value) => {
      this.startAnimationInstant = value;
    });

    this._self.messages.on("releasePlayButton", (release) => {
      this.releasedPlayButton = release;
      if (this.releasedPlayButton === true) {
        this.element.find(".animation__main").css("cursor", "auto");
      } else {
        this.element.find(".animation__main").css("cursor", "wait");
      }
    });

    this._self.messages.on("newSelectionEnd", (value) => {
      this.stopAnimationInstant = value;
    });

    jQuery("body")
      .on("keydown", (event) => {
        if ((event.ctrlKey || event.shiftKey) && !this._self.animation) {
          this._self.doRepeatLastEvent = true;
          this._self.update();
        }
      })
      .on("keyup", () => {
        this._self.doRepeatLastEvent = false;
        this._self.update();
      });
  }

  events() {
    return {
      "click .main__start": "toggle",
      "click .main__pause": "toggle",
      "click .animation__streaming-on": "toggleStreaming",
      "click .animation__streaming-off": "toggleStreaming",
      "click .main__repeat": "repeat",
      "click .animation__stop": "stopButton",
      "click .animation__previous": "previousEvent",
      "click .animation__next": "nextEvent",
      "mouseover .controls__open-button": "mouseOverFlag",
      "mouseout .controls__open-button": "mouseOutFlag",
      "click .controls__open-button": "clickOnFlag",
      "click .controls__discard": "discardConfig",
      "click .controls__save": "updateConfig",
      "click .prefix-label__more": "morePrefixTextbox",
      "click .input__delete": "lessPrefixTextbox",
    };
  }

  /**
   * This method draws this module (eg. inject the DOM and elements).
   */
  render() {
    this._self.element.append(this.element);

    this._updateControls();
  }

  _updateControls() {
    if (this._self.showAnimationControls) {
      this.element.find(".animation__previous svg").hide();
      this.element.find(".animation__next").hide();
      this.element.find(".animation__main").find(".main__start").hide();

      this.element.find(".animation__main").find(".main__pause").show();
      this.element.find(".animation__stop").show();

      this.element.find(".animation__main").find(".main__repeat").hide();
    } else {
      this.element.find(".animation__stop").hide();
      this.element.find(".animation__main").find(".main__pause").hide();

      if (this._self.doRepeatLastEvent) {
        this.element.find(".animation__main").find(".main__start").hide();
        this.element.find(".animation__main").find(".main__repeat").show();
      } else {
        this.element.find(".animation__main").find(".main__start").show();
        this.element.find(".animation__main").find(".main__repeat").hide();
      }

      this.element.find(".animation__previous svg").show();
      this.element.find(".animation__next").show();
    }

    if (this._self.streamingOn) {
      this.element.find(".animation__streaming-on").show();
      this.element.find(".animation__streaming-off").hide();
    } else {
      this.element.find(".animation__streaming-on").hide();
      this.element.find(".animation__streaming-off").show();
    }
  }

  /**
   * If this method is invoked during an animation then the animation pauses otherwise the animation starts.
   */
  toggle() {
    this._self.streamingOn = false;
    if (!this.releasedPlayButton) {
      return;
    }

    this.closeFlag();
    if (this._self.instant.timestamp < this.stopAnimationInstant.timestamp) {
      this.animation = !this.animation;
      this._self.messages.dispatch("animate", this.animation);
    }
    this._updateControls();
  }

  /**
   * This method turn off and on the streaming graph update
   */
  toggleStreaming() {
    this.closeFlag(); // Close panel and stop animation
    if (this.animation) {
      this.animation = false;
      this._self.messages.dispatch("animate", this.animation);
    }

    this._self.streamingOn = !this._self.streamingOn;

    this._updateControls(); // Update icons status
  }

  /**
   * This method reloads the animation.
   */
  reload() {
    this._self.instant = this.startAnimationInstant;
    this._self.messages.dispatch("checkPathPosition");
  }

  /**
   * This method repeats the last one event.
   */
  repeat() {
    let isInitial;

    this._self.messages.dispatch("controlsview:changed:anmiateevent", false);
    isInitial = this.previousEvent() === false;
    this._self.messages.dispatch("controlsview:changed:anmiateevent", true);

    if (!isInitial) {
      this.nextEvent();
    }
  }

  /**
   * This method stops and reloads the animation.
   */
  stopButton() {
    this.stop();
    this.reload();
  }

  /**
   * This method stops the animation.
   */
  stop() {
    this.animation = false;
    this._self.messages.dispatch("animate", false);
    this._updateControls();
  }

  /**
   * This method applies the previous event.
   */
  previousEvent() {
    let prevInstant, prevEvent, instant;
    if (this._self.streamingOn) {
      this._self.streamingOn = false;
      this._updateControls();
    }

    instant = this._self.instant;
    prevEvent = this._self.events.nearest(instant, false, false);
    if (prevEvent !== null) {
      prevInstant = prevEvent.instant;
      if (
        !this._self.config.controller.disableNotSelectedInstants ||
        this._self.events.compare(prevInstant, this.startAnimationInstant) >= 0
      ) {
        this._self.instant = prevInstant;
      }
    } else {
      return false;
    }
  }

  /**
   * This method applies the next event.
   */
  nextEvent() {
    if (this._self.streamingOn) {
      this._self.streamingOn = false;
      this._updateControls();
    }

    let instant = this._self.instant;
    let nextEvent = this._self.events.nearest(instant, true, false);

    if (nextEvent !== null) {
      let nextInstant = nextEvent.instant;
      if (
        !this._self.config.controller.disableNotSelectedInstants ||
        this._self.events.compare(nextInstant, this.stopAnimationInstant) <= 0
      ) {
        this._self.instant = nextInstant;
      }
    }
  }

  mouseOverFlag() {
    if (this.slideOpened === false)
      this.element.find(".controls__open-button").attr("src", openSlideImage);
  }

  mouseOutFlag() {
    if (this.slideOpened === false)
      this.element.find(".controls__open-button").attr("src", configImage);
  }

  clickOnFlag() {
    if (this.slideOpened === false) {
      this.openFlag();
    } else {
      this.discardConfig();
    }
  }

  openFlag() {
    if (this.slideOpened === false) {
      let self = this;
      this.element.animate({ height: "+=280" }, 600, () => {
        this._self.element.animate({ width: "+=150" }, 300, () => {
          this._self.element[0].style.opacity = "1";
          this._self.controlPanelDivComplete.show();
          this._self.controlPanelDivFlagIco.attr("src", closeSlideImage);
        });
      });
      this.slideOpened = true;
    }
  }

  closeFlag() {
    if (this.slideOpened === true) {
      let self = this;
      this.element.find(".controls__other").hide();
      this.element.animate({ height: "-=280" }, 600, () => {
        this._self.element.animate({ width: "-=150" }, 300, () => {
          if (window.innerWidth < 600) {
            this._self.element[0].style.opacity = "0.4";
          }
          this._self.controlPanelDivFlagIco.attr("src", configImage);
        });
      });
      this.slideOpened = false;
    }
  }

  morePrefixTextbox() {
    let element = this.element.find(".resource__prefix-label>div>div").first();
    let parent = element.parent();
    element.clone().appendTo(parent).find("input").val("");
  }

  lessPrefixTextbox(event) {
    let element = $(event.target);
    if (this.element.find(".resource__prefix-label>div>div").length > 1) {
      element.parent().remove();
    } else {
      element.parent().find("input").val("");
    }
  }

  /**
   * This method discards the new query parameters
   */
  discardConfig() {
    this.closeFlag();
    this.render();
  }

  /**
   * This method applies the new query parameters
   */
  updateConfig() {
    if (!this._self.preventNewQueries === false) {
      this.discardConfig();
      return;
    }
    if (this._self.showResourceController) {
      this._self.targets = [];
      this.dom.find(".resource__prefix-label input[type=text]").each(() => {
        this._self.prefixes.push($(this).val());
      });
    }

    let rrcSelected = [];

    this.element.find("input[name=select__input]:checked").each(() => {
      rrcSelected.push($(this).val());
    });

    this._self.ignoreReannouncements = this.element
      .find(".suppress-reannounce__input")
      .is(":checked");

    let message = {
      bgplaysource: true,
      rrcs: rrcSelected,
      ignoreReannouncements: this._self.ignoreReannouncements,
    };

    window.top.postMessage(message, "*");

    if (this._self.thisWidget !== null) {
      let internalParams = {};

      internalParams.targets = this._self.targets.join(",");
      internalParams.starttime = null; // TODO: We have removed the internal datatimepicker. We need to ingest the date from the Widget API.
      internalParams.endtime = null; // TODO: We have removed the internal datatimepicker. We need to ingest the date from the Widget API.
      internalParams.ignoreReannouncements = this._self.ignoreReannouncements;
      internalParams.selectedRrcs = rrcSelected.join(",");
      internalParams.instant = null;

      externalParams = this._self.jsonWrap.setParams(internalParams);

      if (!areMapsEquals(internalParams, this._self, ["instant"])) {
        this._self.oldParams = this._self;
        document.location =
          "?resource=" +
          internalParams.targets +
          "&starttime=" +
          internalParams.starttime +
          "&endtime=" +
          internalParams.endtime +
          "&ignoreReannouncements=" +
          internalParams.ignoreReannouncements +
          "&rrcs=" +
          internalParams.selectedRrcs;
      } else {
        this.discardConfig();
      }
    }
  }
}

export default ControlsView;
