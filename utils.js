// utils.js

function setNested(obj, pathArr, value) {
  let cur = obj;
  for (let i = 0; i < pathArr.length; i++) {
    const p = pathArr[i];
    if (i === pathArr.length - 1) {
      // final
      cur[p] = value;
    } else {
      if (cur[p] == null || typeof cur[p] !== 'object') cur[p] = {};
      cur = cur[p];
    }
  }
}

/**
 * Compute percentages and print age distribution.
 * ageCounters: {lt20: <n>, between20_40: <n>, between40_60: <n>, gt60: <n>}
 */
function printAgeDistribution(ageCounters) {
  const total = Object.values(ageCounters).reduce((a, b) => a + b, 0);
  if (total === 0) {
    console.log("No records processed; cannot compute distribution.");
    return;
  }

  function pct(n) {
    return ((n / total) * 100).toFixed(2);
  }

  console.log("\n=== Age distribution report ===");
  console.log("Age-Group\t\t% Distribution\tCount");
  console.log(`< 20\t\t\t${pct(ageCounters.lt20)}%\t\t${ageCounters.lt20}`);
  console.log(`20 to 40\t\t${pct(ageCounters.b20_40)}%\t\t${ageCounters.b20_40}`);
  console.log(`40 to 60\t\t${pct(ageCounters.b40_60)}%\t\t${ageCounters.b40_60}`);
  console.log(`> 60\t\t\t${pct(ageCounters.gt60)}%\t\t${ageCounters.gt60}`);
  console.log("================================\n");
}

module.exports = { setNested, printAgeDistribution };
