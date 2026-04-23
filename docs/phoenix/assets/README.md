# Phoenix 图资源索引

本目录用于存放从 `docs/phoenix/*.md` 中 Mermaid 代码块导出的图资源。

目录约定：

- `mmd/`：抽取后的 Mermaid 源文件。
- `svg/`：适合文档、幻灯片和网页嵌入的矢量图。
- `png/`：适合汇报稿、IM 发送和不支持 SVG 的场景。

## 生成方式

使用本仓库中的脚本：

```bash
node docs/phoenix/scripts/export_mermaid.mjs
```

脚本默认会：

1. 扫描 `docs/phoenix/*.md`
2. 抽取所有 Mermaid 代码块
3. 写出 `mmd/`
4. 渲染为 `svg/` 与 `png/`

## 依赖

脚本依赖 `@mermaid-js/mermaid-cli`，并使用本机 Chrome 进行渲染。

如果本机没有 `mmdc`，可通过：

```bash
PUPPETEER_EXECUTABLE_PATH="/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" \
npx -y @mermaid-js/mermaid-cli -h
```

先确认渲染器可用。
