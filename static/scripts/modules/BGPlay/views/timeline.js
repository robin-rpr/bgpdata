import { addOffset, arrayContains } from "../utils/miscellaneous.js";
import arrowRightSolidImage from "../assets/images/arrow-right-solid.png";
import leftSliderImage from "../assets/images/leftSlider.png";
import rightSliderImage from "../assets/images/rightSlider.png";
import { calculateAmountOfTime } from "../utils/miscellaneous.js";
import Mustache from "mustache";

const template = `
<div>
    <div class="timeline__controls" style="width:{{controlChartWidth}}px;">
        <div draggable="true" class="draggable controls__slider controls__slider--left"></div>
        <div draggable="true" class="draggable controls__slider controls__slider--right"></div>
        <div class="controls__details">
            <div class="details__text"></div>
            <div class="details__date"></div>
        </div>
        <canvas class="controls__canvas" width="{{controlChartWidth}}" height="{{controlHeight}}">Your browser doesn't support HTML5 Canvas.</canvas>
    </div>

    <div class="timeline__selection">
        <canvas class="selection__canvas" width="{{selectionWidth}}" height="{{selectionHeight}}">Your browser doesn't support HTML5 Canvas.</canvas>
        <div class="selection__controls">
          <svg class="controls__next" alt="Next" title="Next" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 192 512"><path d="M166.9 264.5l-117.8 116c-4.7 4.7-12.3 4.7-17 0l-7.1-7.1c-4.7-4.7-4.7-12.3 0-17L127.3 256 25.1 155.6c-4.7-4.7-4.7-12.3 0-17l7.1-7.1c4.7-4.7 12.3-4.7 17 0l117.8 116c4.6 4.7 4.6 12.3-.1 17z"/></svg>
          <svg class="controls__previous" alt="Previous" title="Previous" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 192 512"><path d="M25.1 247.5l117.8-116c4.7-4.7 12.3-4.7 17 0l7.1 7.1c4.7 4.7 4.7 12.3 0 17L64.7 256l102.2 100.4c4.7 4.7 4.7 12.3 0 17l-7.1 7.1c-4.7 4.7-12.3 4.7-17 0L25 264.5c-4.6-4.7-4.6-12.3.1-17z"/></svg>
        <div>
    </div>
</div>
`;

class TimelineView {
  page = 0;
  pageSize = 40;
  pages = [];
  legend = [];
  disabled = false;
  selectionEvent = undefined;
  seekTimers = [];
  animation = false;
  controlHeight = 50;
  selectionHeight = 70;
  cursorWidth = 2;
  rulerHeight = 6;
  rulerWidth = 6;
  warpWidth = 52;
  pixelPerSecond = 18;
  selectionWidth = 1950;

  /**
   * The initialization method of this object.
   * @param {Map} A map of parameters
   */
  constructor(self) {
    this._self = self;

    this.element = jQuery("<div>")
      .addClass("timeline")
      .append(Mustache.render(template, this));

    this.start = this._self.starttime;
    this.end = this._self.endtime;

    this.selectionStart = this.start;
    this.selectionEnd = this.end;

    this.selectionWidth = this.element.width() - 90;
    this.controlWidth = this.element.width();

    this.detailsDateElement = this.element.find(".details__date");
    this.detailsTextElement = this.element.find(".details__text");
    this.controlCanvasElement = this.element.find(".controls__canvas");
    this.selectionCanvasElement = this.element.find(".selection__canvas");
    this.timelineSelectionElement = this.element.find(".timeline__selection");
    this.controlsSliderElement = this.element.find(".controls__slider");
    this.controlsNextElement = this.element.find(".controls__next");
    this.controlsPreviousElement = this.element.find(".controls__previous");
    this.controlsSliderLeft = this.element.find(".controls__slider--left");
    this.controlsSliderRight = this.element.find(".controls__slider--right");

    this.controlCanvasContext = this.controlCanvasElement[0].getContext("2d", {
      willReadFrequently: true,
    });

    this.selectionCanvasContext = this.selectionCanvasElement[0].getContext(
      "2d",
      {
        willReadFrequently: true,
      },
    );

    this._initPages();
    this._initMessageListeners();
    this._initDOMListeners();
  }

  _initPages() {
    let events = [];

    this._self.events.forEachKey((key) => {
      if (!arrayContains(events, key.timestamp)) {
        events.push(key.timestamp);
      }
    });

    const pagesLength = Math.ceil(events.length / this.pageSize);

    for (let i = 0; i < pagesLength; i++) {
      this.pages[i] = this._self.events.nearest(
        new Instant({ id: 0, timestamp: events[i * this.pageSize] }),
        true,
      );
    }
  }

  _initMessageListeners() {
    this._self.messages.on("animate", (start) => {
      this.stopAnimation();
      this.animation = start;
      this.animate();
    });

    this._self.messages.on("newSample", (instant) => {
      if (this._self.streamingOn) {
        if (this.streamTimeout) {
          clearTimeout(this.streamTimeout);
        }

        this.streamTimeout = setTimeout(() => {
          for (let i = 0; i < this.pages.length - 1; i++) {
            if (this.pages[i + 1].instant.timestamp > instant.timestamp) {
              this._drawControl();
              this._drawSelection(this.selectionChartPages(true)[i], true);
              break;
            }
          }

          this._self.instant = instant;
        }, 200);
      }
    });

    this._self.messages.on("graphanimation:finished", () => {
      this.controlCanvasElement.css("cursor", "pointer");
      this.selectionCanvasElement.css("cursor", "pointer");
      this.disabled = false;
      this.animate();
    });

    this._self.messages.on("graphanimation:performed", (value) => {
      if (value === false) {
        this.controlCanvasElement.css("cursor", "wait");
        this.selectionCanvasElement.css("cursor", "wait");
        this.disabled = true;
      }
    });

    this._self.messages.on("instant:changed", () => {
      this._scrollSelection(this._self.instant);
      this.update();
    });
  }

  _initDOMListeners() {
    this.controlsSliderElement.draggable({
      axis: "x",
      stop: () => {
        if (this.animation) return;

        const confirmAfter = 0; // Delay (in seconds) between a request and an update to prevent flooding.

        this.selectionCanvasElement.hide();

        if (this.selectionAntifloodTimer) {
          clearTimeout(this.selectionAntifloodTimer);
        }

        /**
         * Updates the selection canvas after the specified anti-flood delay.
         */
        const updateSelectionCanvasAntiFlood = () => {
          const pixelToTimeFactor =
            (this._self.endtime - this._self.starttime) / this.controlWidth;

          const newTimestampLeft =
            Math.round(0 * pixelToTimeFactor) + this._self.starttime;
          const newTimestampRight =
            Math.round(this.controlWidth * pixelToTimeFactor) +
            this._self.starttime;

          const numEvents = this._getAmountOfEventsBetween(
            newTimestampLeft,
            newTimestampRight,
          );

          this.detailsTextElement.html(
            `From ${dateToUTCString(newTimestampLeft)} to ${dateToUTCString(
              newTimestampRight,
            )} [${numEvents} events]`,
          );

          this._self.messages.dispatch("releasePlayButton", true);

          updateTimestamps(newTimestampLeft, newTimestampRight);
        };

        /**
         * Updates the start and end timestamps for the selection.
         * @param {number} newTimestampLeft The new start timestamp.
         * @param {number} newTimestampRight The new end timestamp.
         */
        const updateTimestamps = (newTimestampLeft, newTimestampRight) => {
          if (this.selectionStart.timestamp !== newTimestampLeft) {
            this.selectionStart = newTimestampLeft;
            this._self.messages.dispatch(
              "newSelectionStart",
              this.selectionStart,
            );
            this.instant = this.selectionStart;
          }

          if (this.selectionEnd.timestamp !== newTimestampRight) {
            this.selectionEnd = newTimestampRight;
            this._self.messages.dispatch("newSelectionEnd", this.selectionEnd);

            if (newTimestampRight <= this._self.instant.timestamp) {
              this.instant = this.selectionStart;
            } else {
              this._updateSelection();
              this._updateControl();
            }
          }

          this.selectionCanvasElement.show();
        };

        this.selectionAntifloodTimer = setTimeout(
          updateSelectionCanvasAntiFlood.bind(this),
          confirmAfter * 1000,
        );
      },
      drag: (event) => this.checkSliderSelection(event),
    });

    this.selectionCanvasElement.draggable({
      axis: "x",
      drag(event) {
        var container, element, elementLeft, elementRight;

        element = jQuery(event.target);
        container = element.parent();
        elementLeft = element.position().left;
        elementRight = element.position().left + element.width();

        if (elementLeft > 0) {
          element.css("left", "0");
          return false;
        }
        if (elementRight < container.width()) {
          element.css("left", container.width() - element.width());
          return false;
        }
      },
    });

    this.controlCanvasElement.on("click", (event) => {
      event.preventDefault();

      this.stopAnimation();
      this._self.messages.dispatch("animationEnd");

      if (!this.disabled) {
        event = addOffset(event, true);

        if (0 < event.offsetX && event.offsetX < this.controlWidth) {
          const timestamp =
            Math.round(
              event.offsetX *
                ((this._self.endtime - this._self.starttime) /
                  this.controlWidth),
            ) + this._self.starttime;

          this.instant = timestamp;
        }
      }
    });

    this.selectionCanvasElement.on("click", (event) => {
      event.preventDefault();

      this._self.streamingOn = false;

      if (!this.disabled) {
        event = addOffset(event, true);

        const offsetX = event.offsetX;
        const tmpEvent = this.selectionEvent.nearest(offsetX, false, true);

        if (
          tmpEvent.instant.timestamp >= this.selectionStart.timestamp &&
          tmpEvent.instant.timestamp <= this.selectionEnd.timestamp
        ) {
          if (
            tmpEvent !== null &&
            offsetX <
              tmpEvent.drawEventOnselectionCanvasX +
                tmpEvent.drawEventOnselectionCanvasWidth
          ) {
            this.stopAnimation();

            this._self.messages.dispatch("animationEnd");
            this.instant = tmpEvent.instant;
          }
        }
      }
    });

    this.controlsNextElement.on("click", (event) => {
      event.preventDefault();

      this.page++;
      this._drawSelection(this.pages[this.page]);

      this._updateSelection();
    });

    this.controlsPreviousElement.on("click", (event) => {
      event.preventDefault();

      this.page--;
      this._drawSelection(this.pages[this.page]);

      this._updateSelection();
    });
  }

  /**
   * This method draws this module (eg. inject the DOM and elements).
   */
  render() {
    this._self.element.append(this.element);

    this._drawControl();
    this._drawSelection();
    this._updateControl();

    this.timelineSelectionElement.width(this.element.width() - 90);
  }

  /**
   * This method returns the next event in the timeline.
   * @param {Instant} instant An instance of Instant
   * @param {boolean} [forceNotCumulative=false] If this optional parameter is true then the next event will be the first of the next block
   * @returns {Event} An instance of Event
   */
  nextEvent(instant, forceNotCumulative = false) {
    let nextInstant;
    let nextEvent;

    if (
      this._self.config.cumulativeAnimations &&
      forceNotCumulative === false
    ) {
      nextEvent = this.nextEvent(instant, true);

      if (nextEvent !== null) {
        nextInstant = new Instant({
          id: 0,
          timestamp: nextEvent.instant.timestamp + 1,
        });
        nextEvent = this._self.events.nearest(nextInstant, true, true);

        if (nextEvent !== null) {
          nextInstant = new Instant({
            id: nextEvent.instant - 1,
            timestamp: nextEvent.instant.timestamp,
          });
        } else {
          nextInstant = new Instant({
            id: 0,
            timestamp: this._self.endtime,
          });
        }
        nextEvent = this._self.events.nearest(nextInstant, false, true);
      }
    } else {
      nextEvent = this._self.events.nearest(instant, true, false);
    }

    return nextEvent;
  }

  /**
   * This method manages the animation of the time cursor along the timeline.
   * @private
   * @param {Object} instant An instance of Instant which will be the final position of the time cursor
   * @param {Float} delay The duration of the animation
   */
  _seek(instant, delay) {
    // Seconds to milliseconds
    delay = delay * 1000;

    const curTimestamp = this._self.instant.timestamp;
    const timeOffset = instant.timestamp - curTimestamp;

    if (timeOffset <= 0) {
      this._self.instant = instant;
      return null;
    }

    const fps = 25;
    const interval = Math.ceil(1000 / fps);
    const totalFrames = Math.ceil(delay / interval);

    if (totalFrames < 1) {
      totalFrames = 1;
    }

    for (let i = 1; i <= totalFrames; i++) {
      /*
       * Important: if we don't store the pointers to the timers then we can't stop animation in any way
       */
      this.seekTimers.push(
        setTimeout(() => {
          let newTimestamp = curTimestamp + (timeOffset / totalFrames) * i;

          if (totalFrames === i) {
            if (this._self.events.compare(instant, this.selectionEnd) >= 0) {
              this.animation = false;
              this._self.messages.dispatch("animationEnd");
              this._self.instant = this.selectionEnd;
            } else {
              this._self.instant = instant;
            }
          } else {
            /*
             * When totalFrames!=i the cursor has reached a intermediate position calculated
             * by the seek function itself to emulate a fluid animation.
             */
            this._self.instant = new Instant({
              id: 0,
              timestamp: newTimestamp,
            });
            this._updateSelection();
            this._updateControl();
          }
        }, interval * i),
      );
    }
  }

  /**
   * Starts the animation of the timeline.
   * The animation of the timeline is a set of consecutive seek invocations.
   */
  animate() {
    if (!this.animation) {
      this.stopAnimation();
      return;
    }

    const nextEvent = this.nextEvent(this._self.instant, false);

    if (nextEvent === null) {
      stopAnimationAndSeekToEnd();
    } else {
      seekToNextEvent(nextEvent);
    }

    /**
     * Stops the animation and seeks to the end of the timeline.
     */
    const stopAnimationAndSeekToEnd = () => {
      this.animation = false;
      const timestampOffsetToEnd =
        this.selectionEnd.timestamp - this._self.instant.timestamp;
      const seekTime = Math.ceil(getLogarithmicSeekTime(timestampOffsetToEnd));
      this._seek(this.selectionEnd, seekTime);
    };

    /**
     * Seeks to the next event on the timeline.
     * @param {Object} nextEvent The next event object.
     */
    const seekToNextEvent = (nextEvent) => {
      const timestampOffsetToNextEvent =
        nextEvent.instant.timestamp - this._self.instant.timestamp;

      let seekTime;

      if (timestampOffsetToNextEvent < 2) {
        this._seek(nextEvent.instant, 0);
      } else {
        seekTime = Math.ceil(
          getLogarithmicSeekTime(timestampOffsetToNextEvent),
        );
        this._seek(nextEvent.instant, seekTime);
      }
    };

    /**
     * Calculates the duration for a seek operation using a logarithmic approach.
     * @param {number} offset A time interval in seconds.
     * @returns {number} A time interval in seconds.
     */
    const getLogarithmicSeekTime = (offset) => {
      return (
        Math.log(Math.sqrt(Math.sqrt(offset))) /
        Math.log(10) /
        this._self.animationSpeed
      );
    };
  }

  /**
   * This method returns the number of events occurring between two unix timestamps.
   * @private
   * @param {number} start A unix timestamp
   * @param {number} end A unix timestamp
   * @returns {number} The number of events
   */
  _getAmountOfEventsBetween(start, end) {
    var subOrderedMap = this._self.events.getSubTreeMap(
      new Instant({ id: 1, timestamp: start }),
      new Instant({ id: 0, timestamp: end + 1 }),
    );

    return subOrderedMap ? subOrderedMap.size() : 0;
  }

  /**
   * Draws a control chart representing events over time.
   */
  _drawControl() {
    const startTime = this._self.starttime;
    const endTime = this._self.endtime;
    const ctx = this.controlCanvasContext;

    /**
     * Calculates the number of intervals and the time unit.
     * @returns {Object} The number of intervals and the unit of time.
     */
    const calculateTimeIntervals = () => {
      const unit2time =
        (endTime - startTime) / (this.controlWidth / this.rulerWidth);
      const numberOfIntervals = Math.ceil((endTime - startTime) / unit2time);
      return { numberOfIntervals, unit2time };
    };

    /**
     * Draws the ruler on the control chart.
     * @param {number} numberOfIntervals The number of intervals on the ruler.
     * @param {number} rulerNotchWidth The width of each notch on the ruler.
     * @param {number} rulerUnitHeight The height of each unit on the ruler.
     */
    const drawRuler = (numberOfIntervals, rulerNotchWidth, rulerUnitHeight) => {
      const rulerPosition = this.controlHeight - rulerUnitHeight;

      for (let i = 0; i < numberOfIntervals; i++) {
        const ntime2pixel = i * this.rulerWidth;
        ctx.fillStyle = "#000000";
        ctx.fillRect(
          ntime2pixel,
          rulerPosition,
          rulerNotchWidth,
          rulerUnitHeight,
        );
      }
    };

    /**
     * Calculates the peak number of events in any given interval.
     * @returns {number} The peak number of events.
     */
    const calculatePeakEvents = () => {
      let max = 0;

      for (let i = 0; i < numberOfIntervals; i++) {
        const current = this._getAmountOfEventsBetween(
          this._self.starttime + i * unit2time,
          this._self.starttime + i * unit2time + unit2time,
        );

        if (current > max) {
          max = current;
        }
      }

      return max;
    };

    /**
     * Draws the event graph on the control chart.
     * @param {number} eventHeight The height of an event on the graph.
     * @param {string} lineColor The color of the line on the graph.
     * @param {number} lineWidth The width of the line on the graph.
     */
    const drawEventGraph = (eventHeight, lineColor, lineWidth) => {
      // FIXME: This function needs to be urgently optimized. Most of rendering time is lost in this function.

      const graph2YZero = this.controlHeight - this.rulerHeight - 2;
      const eventAmounts = []; // Store the precomputed event amounts

      // Precompute event amounts
      for (let i = 0; i < numberOfIntervals; i++) {
        const startTime = this._self.starttime + i * unit2time;
        const endTime = startTime + unit2time;
        eventAmounts.push(this._getAmountOfEventsBetween(startTime, endTime));
      }

      ctx.beginPath();
      ctx.moveTo(0, graph2YZero);

      // Draw lines based on precomputed values
      eventAmounts.forEach((numOfEvents, i) => {
        const pointY = Math.abs(eventHeight * numOfEvents - graph2YZero);
        ctx.lineTo(i * this.rulerWidth, pointY);
      });

      ctx.lineWidth = lineWidth;
      ctx.strokeStyle = lineColor;
      ctx.stroke();
    };

    const { numberOfIntervals, unit2time } = calculateTimeIntervals(
      endTime - startTime,
    );

    drawRuler(numberOfIntervals, 0.8, this.rulerHeight);

    drawEventGraph(
      numberOfIntervals,
      unit2time,
      this._self.starttime,
      this.controlHeight +
        20 -
        this.rulerHeight /
          calculatePeakEvents(
            numberOfIntervals,
            unit2time,
            this._self.starttime,
          ),
      "blue",
      0.6,
    );
  }

  /**
   * Draws the selection timeline chart.
   *
   * @param {Object} nextFirstStep The event to start drawing the timeline.
   * @param {boolean} [keepPosition=false] Whether to keep the current position in the chart.
   */
  _drawSelection(nextFirstStep, keepPosition = false) {
    const ctx = this.selectionCanvasContext;
    const chartHeight = this.selectionHeight;

    let nextEvent = nextFirstStep || this._self.events.at(0);
    let position = 0;
    let remainingEvents = this.pageSize;
    let sameTimeEvents = [];

    /**
     * Updates the canvas width.
     *
     * @param {number} newWidth The new width for the canvas.
     * @returns {number} The updated canvas width.
     */
    const updateCanvasWidth = (newWidth) => {
      const canvas = this.selectionCanvasElement[0];
      const ctx = this.selectionCanvasContext;

      // Create a temporary canvas and context to store the current drawings
      const tmpCanvas = document.createElement("canvas");
      const tmpContext = tmpCanvas.getContext("2d");

      // Copy width from canvas.
      tmpCanvas.width = canvas.width;
      tmpCanvas.height = canvas.height;

      tmpContext.drawImage(canvas, 0, 0);

      // Update the canvas width
      canvas.width = newWidth;

      // Redraw the stored drawings onto the resized canvas
      ctx.drawImage(tmpCanvas, 0, 0);

      return newWidth;
    };

    /**
     * Draws the legend for events.
     *
     * @param {string} eventType The type of the event to draw in the legend.
     * @param {number} x The x-coordinate for drawing the legend box.
     * @param {number} width The width of the legend box.
     */
    const drawLegend = (eventType, x, width) => {
      const ctx = this.selectionCanvasContext;

      // Check if this event type is already in the legend
      if (this.legend.includes(eventType)) {
        return;
      }

      // Add the new event type to the unique event types array
      this.legend.push(eventType);

      // Draw legend box
      switch (eventType) {
        case "withdrawal":
          ctx.fillStyle = "#D81B29";
        case "announce":
          ctx.fillStyle = "#5AB963";
        case "initialstate":
          ctx.fillStyle = "#808080";
        case "reannounce":
          ctx.fillStyle = "#297F29";
        case "pathchange":
          ctx.fillStyle = "#FF8C00";
        case "prepending":
          ctx.fillStyle = "#FFD700";
        default:
          ctx.fillStyle = "#000000";
      }

      ctx.fillRect(x, 0, width, 10);

      // Draw legend text
      ctx.font = "bold 12px 'Public Sans'";
      ctx.fillStyle = "#000000";
      ctx.textBaseline = "top";
      ctx.fillText(eventType, x + width + 5, 0);
    };

    /**
     * Draws events that occur at the same timestamp.
     *
     * @param {Object} context The `this` context from the calling function, containing necessary properties and methods.
     * @param {number} xPos The x-coordinate where the set of events starts.
     * @returns {number} The updated x-coordinate position after drawing.
     */
    const drawSameTimeEvents = (xPos) => {
      const ctx = this.selectionCanvasContext;
      const gapBetweenEvents = 50;

      let nextXPos = xPos;

      for (let i = 0; i < sameTimeEvents.length; i++) {
        const eventType = sameTimeEvents[i].type;
        const eventSize =
          (this.pixelPerSecond - (sameTimeEvents[i].length + 1)) /
          sameTimeEvents[i].length;

        switch (eventType) {
          case "withdrawal":
            ctx.fillStyle = "#D81B29";
          case "announce":
            ctx.fillStyle = "#5AB963";
          case "initialstate":
            ctx.fillStyle = "#808080";
          case "pathchange":
            ctx.fillStyle = "#FF8C00";
          case "prepending":
            ctx.fillStyle = "#FFD700";
          default:
            ctx.fillStyle = "#000000";
        }

        ctx.fillRect(nextXPos, 0, eventSize, eventSize);

        // Update the x-coordinate for the next event
        nextXPos += eventSize + gapBetweenEvents;
      }

      // Return updated x-coordinate
      return nextXPos;
    };

    /**
     * Draws a "temporal warp" (time gap) between events.
     *
     * @param {number} xPos The x-coordinate where the temporal warp starts.
     * @param {number} timeOffset The amount of time that the warp represents.
     * @returns {void}
     */
    const drawWarp = (xPos, timeOffset) => {
      const ctx = this.selectionCanvasContext;

      const arrowHeight = 10; // Height of the arrow head
      const arrowColor = "#aaa";

      if (this._cache === null) {
        const img = new Image();

        img.onload = function () {
          this._cache = img;
          draw(position, timeOffset);
        };

        img.src = arrowRightSolidImage;
      } else {
        draw(xPos, timeOffset);
      }

      /**
       * Draw the time warp arrow and text based on the provided position and time offset.
       * @param {number} position The x-coordinate where the text starts.
       * @param {number} timeOffset The time offset to display.
       */
      const draw = (position, timeOffset) => {
        const textBaseY = this.selectionHeight - 20 / 5;
        const timeAmount = calculateAmountOfTime(timeOffset);

        ctx.fillStyle = "#000000";
        ctx.font = "bold 11px 'Public Sans'";
        ctx.textBaseline = "top";

        position += 15;

        // Draws the arrow shape
        drawArrow(xPos, arrowHeight, arrowColor);

        // Draws the time-span text
        drawTimeText(timeAmount, position, textBaseY);
      };

      /**
       * Draw the time warp arrow based on the provided x-coordinate, arrow height and color.
       * @param {number} xPos The x-coordinate where the arrow starts.
       * @param {number} arrowHeight The height of the arrow head.
       * @param {string} color The color of the arrow.
       */
      const drawArrow = (xPos, arrowHeight, color) => {
        // Set the context properties for the warp arrow
        ctx.strokeStyle = color;
        ctx.lineWidth = 2;

        // Draw the line part of the arrow
        ctx.beginPath();
        ctx.moveTo(xPos, 0);
        ctx.lineTo(xPos + arrowHeight, 0);
        ctx.stroke();

        // Draw the arrow head
        ctx.beginPath();
        ctx.moveTo(xPos + arrowHeight, 0);
        ctx.lineTo(xPos + arrowHeight - arrowHeight, -arrowHeight / 2);
        ctx.lineTo(xPos + arrowHeight - arrowHeight, arrowHeight / 2);
        ctx.closePath();
        ctx.fillStyle = color;
        ctx.fill();
      };

      /**
       * Draws time-related text based on the calculated amount of time.
       * @param {object} timeAmount The calculated amount of time to display.
       * @param {number} x The x-coordinate where the text starts.
       * @param {number} baseY The base y-coordinate for the text.
       */
      const drawTimeText = (timeAmount, x, baseY) => {
        const units = ["days", "hours", "minutes", "seconds"];
        let yPos = baseY;
        for (const unit of units) {
          if (timeAmount[unit] > 0) {
            ctx.fillText(`${timeAmount[unit]} ${unit[0]}`, x, yPos);
            yPos += baseY;
          }
        }
      };
    };

    // Toggle the arrows
    this.controlsPreviousElement.toggle(this.page > 0);
    this.controlsNextElement.toggle(this.page < this.pages.length - 1);

    this.selectionEvent.empty();
    this._cache = null;

    if (!keepPosition) {
      this.selectionCanvasElement.css("left", "0");
    }

    let prevEvent;

    while (nextEvent && remainingEvents > 0) {
      const currentTimestamp = nextEvent.instant.timestamp;

      if (
        sameTimeEvents[0] &&
        sameTimeEvents[0].instant.timestamp !== currentTimestamp
      ) {
        position = drawSameTimeEvents(position);

        drawWarp(
          position,
          currentTimestamp - sameTimeEvents[0].instant.timestamp,
        );

        position += this.warpWidth;
        sameTimeEvents = [];
        remainingEvents--;
      }

      const canvasWidth = updateCanvasWidth(190);
      drawLegend(nextEvent.subType, 100, 90);

      sameTimeEvents.push(nextEvent);
      prevEvent = nextEvent;
      nextEvent = this.nextEvent(nextEvent.instant, true);

      if (position + this.pixelPerSecond + this.warpWidth > canvasWidth) {
        updateCanvasWidth(this.pixelPerSecond + this.warpWidth);
      }
    }

    if (remainingEvents > 0) {
      position = drawSameTimeEvents(position);
    }

    if (!nextEvent) {
      const finalInterval = this._self.endtime - prevEvent.instant.timestamp;
      if (finalInterval > 0) drawWarp(position, finalInterval);
    }

    this.drawIntervalOnSelectionCanvas();

    ctx.fillStyle = "#000000";
    ctx.fillRect(0, chartHeight - 1, this.selectionWidth, 1);
    ctx.fillRect(1, 5, this.pixelPerSecond, 1);
    ctx.fillRect(1, 3, 1, 4);
    ctx.fillRect(this.pixelPerSecond, 3, 1, 4);
    ctx.font = "bold 12px 'Public Sans'";
    ctx.textBaseline = "top";
    ctx.fillText("1 sec", this.pixelPerSecond + 5, 0);
  }

  update() {
    this._updateControl();
    this._updateSelection();
  }

  /**
   * This method updates the representation of the Control Timeline
   */
  _updateControl() {
    const ctx = this.selectionCanvasContext;

    const timeOffsetPosition =
      this._self.instant.timestamp - this._self.starttime;
    const timestampOffsetPosition = timeOffsetPosition + this._self.starttime;

    const setPixelDensity = (canvas) => {
      // Get the device pixel ratio.
      let pixelRatio = window.devicePixelRatio;

      // Get the actual screen (or CSS) size of the canvas.
      let sizeOnScreen = canvas.getBoundingClientRect();

      // Set our canvas size equal to that of the screen size x the pixel ratio.
      canvas.width = sizeOnScreen.width * pixelRatio;
      canvas.height = sizeOnScreen.height * pixelRatio;

      // Shrink back down the canvas CSS size by the pixel ratio, thereby 'compressing' the pixels.
      canvas.style.width = canvas.width / pixelRatio + "px";
      canvas.style.height = canvas.height / pixelRatio + "px";

      let context = canvas.getContext("2d", { willReadFrequently: true });

      context.scale(pixelRatio, pixelRatio);

      return context;
    };

    if (
      timestampOffsetPosition >= this.selectionStart.timestamp &&
      timestampOffsetPosition <= this.selectionEnd.timestamp
    ) {
      ctx = setPixelDensity(ctx);
      ctx.putImageData(this.controlCanvasCache, 0, 0);
      const positionX =
        this.globalCursorTimeOffset * timeOffsetPosition - this.cursorWidth / 2;

      // Draw the margins for the selected interval
      ctx.fillStyle = "blue";
      ctx.fillRect(0, 0, this.cursorWidth, this.selectionHeight);
      ctx.fillRect(
        this.controlWidth,
        0,
        this.cursorWidth,
        this.selectionHeight,
      );

      // Draw the time cursor
      ctx.fillStyle = self.cursorColor;
      ctx.fillRect(positionX, 0, self.cursorsWidth, self.controlHeight);

      this.detailsDateElement.html(
        "Current instant: " + dateToUTCString(this._self.instant.timestamp),
      );
    }
  }

  /**
   * This method updates the representation of the Selection Timeline
   */
  _updateSelection() {
    const ctx = this.selectionCanvasContext;

    let event = this._self.events.get(this._self.instant);

    if (this._cache === null) {
      this._cache = ctx.getImageData(
        0,
        0,
        this.selectionWidth,
        this.selectionHeight,
      );
    } else {
      ctx.putImageData(this._cache, 0, 0);
    }

    this.drawIntervalOnSelectionCanvas();

    let cursorPosition;

    if (event !== null && this.selectionEvent.find(event)) {
      // The cursor is located on an Event.
      ctx.fillStyle = "red";

      cursorPosition =
        event.drawEventOnselectionCanvasX +
        event.drawEventOnselectionCanvasWidth / 2 -
        this.cursorWidth / 2;

      ctx.fillRect(cursorPosition, 0, this.cursorWidth, this.selectionHeight);
    } else {
      // The Cursor is located on a Warp.
      event = this._self.events.nearest(this._self.instant, false, true);
      if (this.selectionEvent.find(event)) {
        cursorPosition =
          event.drawEventOnselectionCanvasX +
          this.warpWidth / 2 +
          this.pixelPerSecond;
      }
      ctx.fillStyle = "red";
      ctx.fillRect(cursorPosition, 0, this.cursorWidth, this.selectionHeight);
    }
  }

  /**
   * This method returns the page of the given event.
   * @param {Instant} instant An instance of Instant
   * @returns {number} The number of the current page
   */
  _findPageOfInstant(instant) {
    let i;

    for (i = 0; i < this.pages.length; i++) {
      if (this.pages[i + 1].instant.timestamp > instant.timestamp) {
        break;
      }
    }

    return i;
  }

  /**
   * This method auto-scrolls the Selection Canvas when the selected instant is represented close to a margin.
   * @param {Instant} instant An instance of Instant
   */
  _scrollSelection(instant) {
    const nearestEvent = this._self.events.nearest(instant, false);

    if (nearestEvent === null) return null;

    /**
     * Calculates the sum and subtraction of visible positions.
     * @param {Object} nearestEvent The nearest event to the given instant
     * @param {number} offsetOfVisibility Offset of visibility
     * @returns {Object} The sum and subtraction of visible positions
     */
    const calculateVisibilityPositions = (nearestEvent, offsetOfVisibility) => {
      const parentElementPosition = Math.abs(
        this.selectionCanvasElement.position().left,
      );
      return {
        sumVisiblePosition:
          nearestEvent.drawEventOnselectionCanvasX -
          parentElementPosition +
          offsetOfVisibility,
        subVisiblePosition:
          nearestEvent.drawEventOnselectionCanvasX -
          parentElementPosition -
          offsetOfVisibility,
      };
    };

    /**
     * Checks if an element is visible in the viewport of the selection canvas.
     * @param {number} sumVisiblePosition Sum of the visible position
     * @param {number} subVisiblePosition Subtraction of the visible position
     * @returns {boolean} True if the element is visible, otherwise false
     */
    const isElementVisible = (sumVisiblePosition, subVisiblePosition) => {
      return (
        sumVisiblePosition <= this.selectionCanvasElement.parent().width() &&
        subVisiblePosition >= 0
      );
    };

    /**
     * Calculates the new left position for the selection canvas based on the given visible positions.
     * @param {number} sumVisiblePosition Sum of the visible position
     * @param {number} subVisiblePosition Subtraction of the visible position
     * @param {number} offsetOfVisibility Offset of visibility
     * @returns {number} New left position
     */
    function calculateNewLeftPosition(
      sumVisiblePosition,
      subVisiblePosition,
      offsetOfVisibility,
    ) {
      let newLeft = 0;
      const parentElementPosition = this.selectionCanvasElement.position().left;

      if (subVisiblePosition < 0) {
        newLeft =
          Math.abs(parentElementPosition) -
          subVisiblePosition +
          parentElementPosition +
          offsetOfVisibility;
      } else if (
        sumVisiblePosition > this.selectionCanvasElement.parent().width()
      ) {
        newLeft =
          parentElementPosition -
          (sumVisiblePosition - this.selectionCanvasElement.parent().width());
      }
      return checkAndCorrectPosition(newLeft);
    }

    /**
     * Checks and corrects the new left position based on the parent element's dimensions and configuration.
     * @param {number} newLeft New left position
     * @returns {number} Corrected new left position
     */
    const checkAndCorrectPosition = (newLeft) => {
      if (newLeft > 0) return 0;

      const minWidth = this.selectionWidth;
      const parentWidth = this.selectionCanvasElement.parent().width();
      if (newLeft + minWidth < parentWidth) {
        return parentWidth - minWidth;
      }
      return newLeft;
    };

    const offsetOfVisibility =
      (this.selectionCanvasElement.parent().width() / 100) * 20;

    const { sumVisiblePosition, subVisiblePosition } =
      calculateVisibilityPositions(nearestEvent, offsetOfVisibility);

    if (isElementVisible(sumVisiblePosition, subVisiblePosition)) return;

    const newLeftPosition = calculateNewLeftPosition(
      sumVisiblePosition,
      subVisiblePosition,
      offsetOfVisibility,
    );

    this.selectionCanvasElement.animate({ left: newLeftPosition }, 500);
  }

  /**
   * This method provides a description for each type of event.
   * @param {String} eventType The type of an event
   */
  getEventVerboseType(eventType) {
    var text, mainType;

    mainType = this._self.type;
    switch (mainType) {
      case "traceroute":
        break;
      default:
        switch (eventType) {
          case "withdrawal":
            text = "Withdrawal";
            break;
          case "announce":
            text = "Announce";
            break;
          case "pathchange":
            text = "Path Change";
            break;
          case "reannounce":
            text = "Re-announce";
            break;
          case "prepending":
            text = "Prepending";
            break;
          case "initialstate":
            text = "Initial state";
            break;
          default:
            text = "event";
        }
    }
    return text;
  }

  /**
   * This method stops a seek process
   */
  stopAnimation() {
    for (let timer of this.seekTimers) {
      clearTimeout(timer);
    }

    // Reset Timers
    this.seekTimers = [];
  }

  /**
   * This method checks and prevents inconsistent selection of the Control Timeline.
   * @param {Event} event The drag event object.
   * @returns {boolean|null} Returns false if the drag should be prevented, null otherwise.
   */
  checkSliderSelection(event) {
    if (this.animation) return false;

    this._self.messages.dispatch("releasePlayButton", false);

    const sliderLeft = this.controlsSliderLeft;
    const sliderRight = this.controlsSliderRight;
    let { left: xLeft } = sliderLeft.position();
    let { left: xRight } = sliderRight.position();

    const sliderLeftWidth = sliderLeft.width();
    const halfSliderLeft = sliderLeftWidth / 2;
    const halfSliderRight = sliderRight.width() / 2;

    const element = jQuery(event.target);

    const adjustSliderPosition = (position, halfWidth, limit) => {
      element.css(
        "left",
        `${position + halfWidth > limit ? limit - halfWidth : position}px`,
      );
      return false;
    };

    if (
      xLeft < -halfSliderLeft ||
      xRight + halfSliderRight > this.controlWidth
    ) {
      return adjustSliderPosition(xLeft, halfSliderLeft, this.controlWidth);
    }

    if (xLeft + sliderLeftWidth > xRight) {
      const targetPosition = element.position().left;
      element.css(
        "left",
        `${
          targetPosition === xLeft
            ? xRight - sliderLeftWidth
            : xLeft + sliderLeftWidth
        }px`,
      );
      return false;
    }

    this.sliderLeft = xLeft + halfSliderLeft;
    this.sliderRight = xRight + halfSliderRight;

    const time =
      (this._self.endtime - this._self.starttime) / this.controlWidth;
    const newTimestampLeft =
      Math.round(this.sliderLeft * time) + this._self.starttime;
    const newTimestampRight =
      Math.round(this.sliderRight * time) + this._self.starttime;

    this.detailsTextElement.html(
      `From ${dateToUTCString(newTimestampLeft)} to ${dateToUTCString(
        newTimestampRight,
      )}`,
    );
  }

  /**
   * This method draws on the Selection Timeline an interval selected on the Control Timeline
   */
  drawIntervalOnSelectionCanvas() {
    const ctx = this.selectionCanvasContext;

    const start = this._self.events.nearest(this.selectionStart, true, true);
    const stop = this._self.events.nearest(this.selectionEnd, false, true);

    const draw = (start, stop) => {
      let positionXL, positionXR, darkened;

      if (this._cache === null) {
        this._cache = ctx.getImageData(
          0,
          0,
          this.selectionWidth,
          this.selectionHeight,
        );
      } else {
        ctx.putImageData(this._cache, 0, 0);
      }

      darkened = false;

      if (
        start !== null &&
        this.selectionStart !== this.start &&
        start.drawEventOnselectionCanvasX !== null &&
        this.selectionEvent.find(start)
      ) {
        positionXL =
          start.drawEventOnselectionCanvasX -
          this.warpWidth / 2 -
          this.selectorLeftImageCache.naturalWidth / 2; // Position of the first event included - the half of the warp - the half of the slider image

        // Draw the margins for the selected interval
        ctx.fillStyle = "blue";
        ctx.fillRect(
          positionXL + this.selectorLeftImageCache.naturalWidth / 2,
          0,
          this.cursorWidth,
          this.selectionHeight,
        );

        ctx.drawImage(this.selectorLeftImageCache, positionXL, 0);
      }

      if (
        stop !== null &&
        this.selectionEnd !== this.end &&
        stop.drawEventOnselectionCanvasX !== null &&
        this.selectionEvent.find(stop)
      ) {
        positionXR =
          stop.drawEventOnselectionCanvasX +
          this.pixelPerSecond +
          this.warpWidth / 2 -
          this.selectorLeftImageCache.naturalWidth / 2; // Position of the first event included + width of the event + the half of the warp - the half of the slider image

        if (positionXR === positionXL) {
          // There are no events in the selected interval
          positionXR += this.selectorLeftImageCache.naturalWidth; // Without this line the two sliders will be overlapped
        }

        // Draw the margins for the selected interval
        ctx.fillStyle = "blue";
        ctx.fillRect(
          positionXR + this.selectorLeftImageCache.naturalWidth / 2,
          0,
          this.cursorWidth,
          this.selectionHeight,
        );

        ctx.drawImage(this.selectorRightImageCache, positionXR, 0);
      }
    };

    if (this.selectorLeftImageCache === null) {
      let imgLeft = new Image();
      imgLeft.onload = function () {
        this.selectorLeftImageCache = imgLeft;
        let imgRight = new Image();
        imgRight.onload = function () {
          this.selectorRightImageCache = imgRight;
          draw(start, stop);
          this._self.messages.dispatch("moduleLoaded", this);
        };
        imgRight.src = rightSliderImage;
      };
      imgLeft.src = leftSliderImage;
    } else {
      draw(start, stop);
    }
  }
}

export default TimelineView;
