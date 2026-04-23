import { existsSync, mkdirSync, readdirSync, readFileSync, writeFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const docsDir = path.resolve(__dirname, "..");
const assetsDir = path.join(docsDir, "assets");
const mmdDir = path.join(assetsDir, "mmd");
const svgDir = path.join(assetsDir, "svg");
const pngDir = path.join(assetsDir, "png");
const puppeteerConfig = path.join(__dirname, "puppeteer-config.json");

const docs = readdirSync(docsDir)
  .filter((name) => name.endsWith(".md"))
  .filter((name) => name !== "assets")
  .sort();

mkdirSync(mmdDir, { recursive: true });
mkdirSync(svgDir, { recursive: true });
mkdirSync(pngDir, { recursive: true });

function slugify(input) {
  return input
    .toLowerCase()
    .replace(/[`*_]/g, "")
    .replace(/[^\p{L}\p{N}]+/gu, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 60) || "diagram";
}

function findNearestHeading(content, offset) {
  const lines = content.slice(0, offset).split("\n");
  for (let i = lines.length - 1; i >= 0; i -= 1) {
    const line = lines[i].trim();
    if (line.startsWith("#")) {
      return line.replace(/^#+\s*/, "");
    }
  }
  return "diagram";
}

function render(sourcePath, outputPath, format) {
  const args = [
    "-y",
    "@mermaid-js/mermaid-cli",
    "-i",
    sourcePath,
    "-o",
    outputPath,
    "-e",
    format,
    "-p",
    puppeteerConfig,
    "-q",
  ];

  if (format === "png") {
    args.push("-w", "2000", "-s", "2", "-b", "white");
  } else if (format === "svg") {
    args.push("-b", "white");
  }

  const result = spawnSync("npx", args, {
    cwd: path.resolve(docsDir, "..", ".."),
    stdio: "pipe",
    env: {
      ...process.env,
      PUPPETEER_EXECUTABLE_PATH:
        process.env.PUPPETEER_EXECUTABLE_PATH ||
        "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
    },
    encoding: "utf8",
  });

  if (result.status !== 0) {
    throw new Error(
      `Failed to render ${path.basename(sourcePath)} -> ${format}\n${result.stderr || result.stdout}`,
    );
  }
}

const manifest = [];

for (const docName of docs) {
  const docPath = path.join(docsDir, docName);
  const content = readFileSync(docPath, "utf8");
  const regex = /```mermaid\n([\s\S]*?)```/g;
  let match;
  let index = 0;

  while ((match = regex.exec(content)) !== null) {
    index += 1;
    const source = `${match[1].trim()}\n`;
    const heading = findNearestHeading(content, match.index);
    const baseName = `${path.basename(docName, ".md")}--${String(index).padStart(2, "0")}--${slugify(heading)}`;
    const sourcePath = path.join(mmdDir, `${baseName}.mmd`);
    const svgPath = path.join(svgDir, `${baseName}.svg`);
    const pngPath = path.join(pngDir, `${baseName}.png`);

    writeFileSync(sourcePath, source, "utf8");

    if (!existsSync(svgPath)) {
      render(sourcePath, svgPath, "svg");
    }

    if (!existsSync(pngPath)) {
      render(sourcePath, pngPath, "png");
    }

    manifest.push({
      doc: docName,
      index,
      heading,
      mmd: path.relative(assetsDir, sourcePath),
      svg: path.relative(assetsDir, svgPath),
      png: path.relative(assetsDir, pngPath),
    });
  }
}

writeFileSync(path.join(assetsDir, "manifest.json"), `${JSON.stringify(manifest, null, 2)}\n`, "utf8");

const lines = [
  "# Phoenix 图资源清单",
  "",
  "| 文档 | 图序号 | 标题 | SVG | PNG |",
  "| --- | --- | --- | --- | --- |",
];

for (const item of manifest) {
  lines.push(
    `| ${item.doc} | ${item.index} | ${item.heading} | [${item.svg}](${item.svg}) | [${item.png}](${item.png}) |`,
  );
}

writeFileSync(path.join(assetsDir, "INDEX.md"), `${lines.join("\n")}\n`, "utf8");

console.log(`Rendered ${manifest.length} diagrams.`);
