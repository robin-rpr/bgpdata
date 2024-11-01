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

/**
 * Represents a message aggregator for inter-service communication.
 * @description Consider using a lowercase string containing only letters and ":", structured like `receiver:action`, for event names.
 * @class MessageAggregator
 */
class MessageAggregator {
  constructor() {
    this._messages = {};
  }

  /**
   * Gets all messages.
   */
  get messages() {
    return this._messages;
  }

  /**
   * Adds a listener for a given event name.
   * @param {string} eventName The name of the event to listen for.
   * @param {function} callback The callback function to execute when the event is triggered.
   */
  on(eventName, callback) {
    if (!this._messages[eventName]) {
      this._messages[eventName] = [];
    }
    this._messages[eventName].push(callback);
  }

  /**
   * Removes a listener from a given event name.
   * @param {string} eventName The name of the event.
   * @param {function} callback The callback function to remove.
   */
  off(eventName, callback) {
    if (!this._messages[eventName]) {
      return;
    }
    const index = this._messages[eventName].indexOf(callback);
    if (index !== -1) {
      this._messages[eventName].splice(index, 1);
    }
  }

  /**
   * Asynchronously dispatches an event.
   * @param {string} eventName The name of the event to dispatch.
   * @param {...*} args The arguments to pass to the listeners.
   */
  dispatch(eventName, ...args) {
    if (!this._messages[eventName]) {
      return;
    }

    for (let callback of this._messages[eventName]) {
      // Use setTimeout to make it asynchronous on the event loop.
      setTimeout(() => callback(...args), 0);
    }
  }
}

export default MessageAggregator;
