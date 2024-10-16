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
