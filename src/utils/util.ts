/**
 * @method isEmpty
 * @param {String | Number | Object} value
 * @returns {Boolean} true & false
 * @description this value is Empty Check
 */
export const isEmpty = (value: string | number | object): boolean => {
  if (value === null) {
    return true;
  } else if (typeof value !== "number" && value === "") {
    return true;
  } else if (typeof value === "undefined" || value === undefined) {
    return true;
  } else if (value !== null && typeof value === "object" && !Object.keys(value).length) {
    return true;
  } else {
    return false;
  }
};

export const maxIP = (a: string, b: string) => {
  // a and b are ip addresses
  const ip1 = a.split(".");
  const ip2 = b.split(".");
  for (let i = 0; i < 4; i++) {
    if (parseInt(ip1[i]) > parseInt(ip2[i])) {
      return a;
    } else if (parseInt(ip1[i]) < parseInt(ip2[i])) {
      return b;
    }
  }

  // they are equal
  return a;
}
