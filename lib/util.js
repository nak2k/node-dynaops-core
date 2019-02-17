function removeUndefined(obj) {
  return Object.entries(obj).reduce(
    (obj, [k, v]) => v !== undefined ? (obj[k] = v, obj) : obj,
    {});
}

exports.removeUndefined = removeUndefined;
