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
export function calculateAmountOfTime(timestamp) {
  let amount = {};

  amount.days = Math.floor(timestamp / 86400); // 86400sec = 1 day.

  let tmp = timestamp % 86400;

  amount.hours = Math.floor(tmp / 3600); // 3600sec = 1 hour.
  tmp = (timestamp % 86400) % 3600;

  amount.minutes = Math.floor(tmp / 60);
  amount.seconds = tmp % 60;

  return amount;
}

export function addOffset(event, forced) {
  const offset = $(event.target).offset();

  if (!event.offsetX || forced) {
    event.offsetX = event.pageX - offset.left;
    event.offsetY = event.pageY - offset.top;
  }

  return event;
}

export function dateToUTCString(timestamp) {
  const date = new Date(timestamp * 1000);

  const year = date.getUTCFullYear();
  const month = String(date.getUTCMonth() + 1).padStart(2, "0"); // months are 0-indexed
  const day = String(date.getUTCDate()).padStart(2, "0");
  const hours = String(date.getUTCHours()).padStart(2, "0");
  const minutes = String(date.getUTCMinutes()).padStart(2, "0");
  const seconds = String(date.getUTCSeconds()).padStart(2, "0");

  return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`;
}

export function areMapsEquals(map1, map2, excludedKey) {
  for (let key in map1) {
    if (
      (excludedKey === null || !arrayContains(excludedKey, key)) &&
      map1[key] !== map2[key]
    ) {
      return false;
    }
  }

  return true;
}

export function arrayContains(array, element) {
  for (let i = 0; i < array.length; i++) {
    if (array[i] === element) {
      return true;
    }
  }

  return false;
}

export function removeSubArray(mainArray, subArray) {
  let tmp = [];

  for (let i = 0; i < mainArray.length; i++) {
    let inArray = false;

    for (let j = 0; j < subArray.length; j++) {
      if (subArray[j] === mainArray[i]) {
        inArray = true;
        break;
      }
    }

    if (inArray === false) {
      tmp.push(mainArray[i]);
    }
  }

  return tmp;
}
