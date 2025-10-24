function parseCSVLineToFields(line) {
  const fields = [];
  let i = 0;
  const len = line.length;
  while (i < len) {
    let ch = line[i];

    if (ch === '"') {
      i++; // skip opening quote
      let field = "";
      while (i < len) {
        if (line[i] === '"') {
          if (i + 1 < len && line[i + 1] === '"') {
            field += '"';
            i += 2;
          } else {
            i++;
            break;
          }
        } else {
          field += line[i++];
        }
      }
      while (i < len && line[i] !== ',') {
        if (line[i] === ' ' || line[i] === '\t') { i++; continue; }
        break;
      }
      fields.push(field);
      if (i < len && line[i] === ',') i++; // skip comma
    } else {
      let start = i;
      while (i < len && line[i] !== ',') i++;
      let raw = line.slice(start, i).trim();
      fields.push(raw);
      if (i < len && line[i] === ',') i++; // skip comma
    }
  }
  return fields;
}

function isRecordComplete(accum) {
  let cnt = 0;
  for (let i = 0; i < accum.length; i++) {
    if (accum[i] === '"') cnt++;
  }
  // If count is odd -> incomplete
  return cnt % 2 === 0;
}

module.exports = {
  parseCSVLineToFields,
  isRecordComplete
};
