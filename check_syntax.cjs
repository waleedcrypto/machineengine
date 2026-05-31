const fs = require("fs");
try {
  new Function(fs.readFileSync("./netlify/app.js", "utf8"));
  console.log("No Syntax Errors in app.js!");
} catch (e) {
  console.error("SYNTAX ERROR in app.js:");
  console.error(e);
}
