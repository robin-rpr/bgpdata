/*** 
BGPDATA - BGP Data Collection and Analytics Service

This software is part of the BGPDATA project, which is designed to collect, process, and analyze BGP data from various sources.
It helps researchers and network operators get insights into their network by providing a scalable and reliable way to analyze and inspect historical and live BGP data from Route Collectors around the world.

Author: Robin Röper

© 2024 BGPDATA. All rights reserved.

Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:
1. Redistributions of source code must retain the above copyright notice, this list of conditions, and the following disclaimer.
2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions, and the following disclaimer in the documentation and/or other materials provided with the distribution.
3. Neither the name of BGPDATA nor the names of its contributors may be used to endorse or promote products derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
***/
import { areMapsEquals, removeSubArray } from "../utils/miscellaneous.js";
import openSlideImage from "../assets/images/openSlide.png";
import closeSlideImage from "../assets/images/closeSlide.png";
import configImage from "../assets/images/config.png";
import streamingOnImage from "../assets/images/bgplay_streaming_on.png";
import streamingOffImage from "../assets/images/bgplay_streaming_off.png";
import playPrevImage from "../assets/images/play_prev.png";
import pauseImage from "../assets/images/pause.png";
import playImage from "../assets/images/play.png";
import stopImage from "../assets/images/stop.png";
import playNextImage from "../assets/images/play_next.png";

const template = `
<img class="bgplayControlPanelDivFlagIco" src="${configImage}" alt="Open config" title="Open config"/>
<div class="bgplayControlAnimationDiv">
    {{#environment.useStreaming}}
    <div class="bgplayControlStreamingOn">
        <img src="${streamingOnImage}" alt="Streaming off" title="Turn streaming off"/>
    </div>
    <div class="bgplayControlStreamingOff">
        <img src="${streamingOffImage}" alt="Streaming on" title="Turn streaming on"/>
    </div>
    {{/environment.useStreaming}}
    <div class="bgplayControlAnimationPrev">
        <img src="${playPrevImage}" alt="Previous event" title="Previous event"/>
    </div>
    <div class="bgplayControlAnimationStartPause">
        <img src="${pauseImage}" alt="Pause animation" title="Pause animation"/>
        <img src="${playImage}" alt="Play animation" title="Play animation"/>
    </div>
    <div class="bgplayControlAnimationStop">
        <img src="${stopImage}" alt="Stop animation" title="Stop animation"/>
    </div>
    <div class="bgplayControlAnimationNext">
        <img src="${playNextImage}" alt="Next event" title="Next event"/>
    </div>
</div>
<div class="bgplayControlPanelDivComplete">
    {{#selectableRrcs}}
    <div class="bgplayControlRRCDiv">
        <label style="float:left; clear:both; width:100%;">Route Collectors: </label>
        {{#selectableRrcsObj}}
        <div class="bgplayRrcsSelect">
            <input name="bgplayRrcSelect" type="checkbox" value="{{id}}" {{#selected}}checked="checked"{{/selected}}/><label>{{label}}</label>
        </div>
        {{/selectableRrcsObj}}
    </div>
    {{/selectableRrcs}}
    <div class="bgplaySuppressReannounceDiv">
        <label for="bgplaySuppressReannounce">Ignore re-announcements</label><input class="bgplaySuppressReannounce" type="checkbox" {{#ignoreReannouncements}}checked="checked"{{/ignoreReannouncements}}/>
    </div>
    <input type="button" class="bgplayControlApplyButton"/>
    <input type="button" class="bgplayControlDiscardButton"/>
</div>
`;

/**
 * Represents a Controller for querying in a BGP module.
 * @class
 */
class ControllerQueryView {
  constructor(self) {
    this.element = jQuery("<div>").addClass("controller").hide();
    self.append(this.element);

    this.events = this._self.messages;
    this.animation = false;
    this.selectedRrcs = this._self.selectedRrcs.split(",");
    this.selectableRrcs = this._self.config.selectableRrcs;
    this.possibleRrcs = removeSubArray(
      this._self.config.possibleRrcs,
      this.selectedRrcs,
    );
    this.slideOpened = false;
    this.showResourceController = false;
    this.ignoreReannouncements =
      this._self.ignoreReannouncements ||
      this._self.config.ignoreReannouncementsByDefault;
    this.releasedPlayButton = true;

    this.render();
  }

  /**
   * Gets the DOM elements associated with this view.
   */
  getDomElements() {
    this.dom = this.$el;
    this.controlAnimationStartPause = this.dom.find(
      ".bgplayControlAnimationStartPause",
    );
    this.controlAnimationStop = this.dom.find(".bgplayControlAnimationStop");
    this.controlAnimationNext = this.dom.find(".bgplayControlAnimationNext");
    this.controlAnimationPrevImage = this.dom.find(
      ".bgplayControlAnimationPrev svg",
    );
    this.starttimestampPicker = this.dom.find(".bgplayStarttimestampPicker");
    this.endtimestampPicker = this.dom.find(".bgplayEndtimestampPicker");
    this.controlPanelDivFlagIco = this.dom.find(
      ".bgplayControlPanelDivFlagIco",
    );
    this.controlPanelDivComplete = this.dom.find(
      ".bgplayControlPanelDivComplete",
    );
    this.suppressReannounce = this.dom.find(".bgplaySuppressReannounce");
  }

  /**
   * This method draws this module (eg. inject the DOM and elements).
   */
  render() {
    this.getDomElements();
    this.dom.show();
    this.controlAnimationStartPause.css("opacity", "0.4");
    this.controlAnimationNext.css("opacity", "0.4");
    this.controlAnimationPrevImage.css("opacity", "0.4");
    this.update();

    return this;
  }

  /**
   * Update the view based on the current state.
   */
  update() {
    let startStopImages;
    startStopImages = this.controlAnimationStartPause.find("img");
    if (this.animation === true) {
      this.controlAnimationPrevImage.hide();
      this.controlAnimationNext.hide();
      startStopImages.eq(1).hide();

      startStopImages.eq(0).show();
      this.controlAnimationStop.show();
    } else {
      this.controlAnimationStop.hide();
      startStopImages.eq(0).hide();

      startStopImages.eq(1).show();
      this.controlAnimationPrevImage.show();
      this.controlAnimationNext.show();
    }
  }

  /**
   * Toggle the view state.
   */
  toggle() {
    if (!this.releasedPlayButton) {
      return;
    }

    this.closeFlag();
    if (this._self.instant.timestamp < this.stopAnimationInstant.timestamp) {
      this.animation = !this.animation;
      this.update();
      this.events.dispatch("animate", this.animation);
    }
  }

  /**
   * Handles the mouse over event on the flag icon.
   */
  mouseOverFlag() {
    if (this.slideOpened === false) {
      this.controlPanelDivFlagIco.attr("src", openSlideImage);
    }
  }

  /**
   * Handles the mouse out event on the flag icon.
   */
  mouseOutFlag() {
    if (this.slideOpened === false) {
      this.controlPanelDivFlagIco.attr("src", configImage);
    }
  }

  /**
   * Handles the click event on the flag icon.
   */
  clickOnFlag() {
    if (this.slideOpened === false) {
      this.openFlag();
    } else {
      this.discardConfig();
    }
  }

  /**
   * Opens the flag to show more options.
   */
  openFlag() {
    if (this.slideOpened === false) {
      this.dom.animate({ height: "+=340" }, 600, () => {
        this.dom.animate({ width: "+=150" }, 300, () => {
          this.controlPanelDivComplete.show();
          this.controlPanelDivFlagIco.attr("src", closeSlideImage);
        });
      });
      this.slideOpened = true;
    }
  }

  /**
   * Closes the flag to hide options.
   */
  closeFlag() {
    if (this.slideOpened === true) {
      let self = this;
      this.controlPanelDivComplete.hide();
      this.dom.animate({ height: "-=340" }, 600, function () {
        self.dom.animate({ width: "-=150" }, 300, function () {
          this.controlPanelDivFlagIco.attr("src", openSlideImage);
        });
      });
      this.slideOpened = false;
    }
  }

  /**
   * Adds more textboxes for prefix input.
   */
  morePrefixTextbox() {
    let element = this.dom.find(".bgplayControlPrefixValue>div>div").first();
    let parent = element.parent();
    element.clone().appendTo(parent).find("input").val("");
  }

  /**
   * Removes the textboxes for prefix input.
   * @param {Event} event The event object.
   */
  lessPrefixTextbox(event) {
    let element = $(event.target);
    if (this.dom.find(".bgplayControlPrefixValue>div>div").length > 1) {
      element.parent().remove();
    } else {
      element.parent().find("input").val("");
    }
  }

  /**
   * Discards the new query parameters.
   */
  discardConfig() {
    this.closeFlag();
    this.render();
  }

  /**
   * Applies the new query parameters.
   */
  updateConfig() {
    if (!this._self.preventNewQueries === false) {
      this.discardConfig();
      return;
    }

    let internalParams, rrcSelected, self, externalParams;

    self = this;
    if (this.showResourceController) {
      this.prefixes = [];
      this.dom
        .find(".bgplayControlPrefixValue input[type=text]")
        .each(function () {
          self.prefixes.push($(this).val());
        });
    }

    rrcSelected = [];
    this.dom.find("input[name=bgplayRrcSelect]:checked").each(function () {
      rrcSelected.push($(this).val());
    });
    let message = { bgplaysource: true, rrcs: rrcSelected };
    window.top.postMessage(message, "*");
    this.ignoreReannouncements = this.suppressReannounce.is(":checked");

    if (this._self.thisWidget !== null) {
      internalParams = {};

      internalParams.targets = this.prefixes.join();
      internalParams.starttime = null; // TODO: We have removed the internal datatimepicker. We need to ingest the date from the Widget API.
      internalParams.endtime = null; // TODO: We have removed the internal datatimepicker. We need to ingest the date from the Widget API.
      internalParams.ignoreReannouncements = this.ignoreReannouncements;
      internalParams.selectedRrcs = rrcSelected.join();

      // The new query will start from the initial instant.
      internalParams.instant = null;

      externalParams = this._self.jsonWrap.setParams(internalParams);

      if (!areMapsEquals(internalParams, this._self)) {
        this._self.oldParams = this._self;
        this._self.thisWidget.navigate(externalParams);

        // If the module was not updated then the query parameters are the same, close the flag.
        this.closeFlag();
        this.events.dispatch("destroyAll");
      } else {
        this.discardConfig();
      }
    }
  }
}
export default ControllerQueryView;
