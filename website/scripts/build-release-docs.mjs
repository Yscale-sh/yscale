import {copyFile, mkdir, readFile, writeFile} from "node:fs/promises";
import path from "node:path";
import {fileURLToPath} from "node:url";
import {DOC_PAGES, DOC_FILES, docPage} from "../src/lib/releaseDocs.js";

const root = fileURLToPath(new URL("..", import.meta.url));
const source = path.join(root, "content/release");
const output = path.join(root, "dist");
for (const [file, url] of DOC_PAGES) {
  const target = path.join(output, url === "/docs/" ? "docs/index.html" : url.slice(1));
  await mkdir(path.dirname(target), {recursive: true});
  await writeFile(target, docPage(file, await readFile(path.join(source, file), "utf8")));
}
for (const file of DOC_FILES) {
  const target = path.join(output, "docs/files", file);
  await mkdir(path.dirname(target), {recursive: true});
  await copyFile(path.join(source, file), target);
}
await copyFile(path.join(root, "src/styles/docs.css"), path.join(output, "docs/docs.css"));
await mkdir(path.join(output, "docs/fonts"), {recursive: true});
for (const [pkg, file, name] of [
  ["hanken-grotesk", "hanken-grotesk-latin-400-normal.woff2", "body"],
  ["bricolage-grotesque", "bricolage-grotesque-latin-700-normal.woff2", "display"],
]) {
  await copyFile(path.join(root, "node_modules/@fontsource", pkg, "files", file), path.join(output, "docs/fonts", `${name}.woff2`));
}
console.log(`Rendered ${DOC_PAGES.length} release documentation pages and ${DOC_FILES.length} reference files.`);
