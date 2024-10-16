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
