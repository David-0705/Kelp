// csvParser.js
// Custom CSV parsing helpers (RFC4180-like)

function parseCSVLineToFields(line) {
  // Parse one CSV *record string* into array of fields.
  // Handles quoted fields containing commas and escaped quotes ("")
  const fields = [];
  let i = 0;
  const len = line.length;
  while (i < len) {
    let ch = line[i];

    if (ch === '"') {
      // Quoted field
      i++; // skip opening quote
      let field = "";
      while (i < len) {
        if (line[i] === '"') {
          // Could be escaped quote or end
          if (i + 1 < len && line[i + 1] === '"') {
            // escaped quote
            field += '"';
            i += 2;
          } else {
            // closing quote
            i++;
            break;
          }
        } else {
          field += line[i++];
        }
      }
      // After closing quote, skip optional whitespace until comma or end
      while (i < len && line[i] !== ',') {
        // Skip single spaces/tabs between closing quote and comma (if any)
        if (line[i] === ' ' || line[i] === '\t') { i++; continue; }
        // If we find other characters, that's malformed, but we'll continue
        break;
      }
      fields.push(field);
      if (i < len && line[i] === ',') i++; // skip comma
    } else {
      // Unquoted field: read until comma
      let start = i;
      while (i < len && line[i] !== ',') i++;
      let raw = line.slice(start, i).trim();
      fields.push(raw);
      if (i < len && line[i] === ',') i++; // skip comma
    }
  }
  return fields;
}

/*
We will read the file line-by-line using readline, but CSV fields
may contain newlines. So at the consumer we must accumulate physical
lines until we have a balanced set of quotes for a record.

This helper checks whether the current accumulator has complete record (even number of unescaped quotes).
*/
function isRecordComplete(accum) {
  // Count number of quotes that are not part of escaped double quotes
  // Easier: count total quotes and subtract double-quote pairs effect.
  // If there are odd number of quotes, record is incomplete.
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
