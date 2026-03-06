# 视频去重与归类工具 (Video Deduplicator)

这是一个使用 Golang 编写的本地视频去重与归类工具。它可以递归扫描指定目录下的视频文件，通过哈希摘要（SHA-256）和采样相似度比对来识别重复视频，并根据文件夹结构将视频归类到“省份/城市”目录中。

## 功能特性

*   **双重去重机制**：
    *   **精确去重**：基于文件全量 SHA-256 哈希值，精准识别完全相同的文件。
    *   **近重复去重**：基于文件采样块的相似度比对，识别内容高度相似的视频（如不同分辨率、元数据差异等）。
*   **智能归类**：
    *   根据输入文件的目录层级（如 `.../浙江省/杭州市/video.mp4`）自动解析省份和城市信息。
    *   **回落机制**：如果省份或城市无法识别（或为“未知”），则按**原有目录结构**保持归档，不会强制放入“待分类”文件夹。
*   **非视频文件处理**：
    *   扫描到的非视频文件（图片、文档等）不参与去重分析，但会**保持原有目录结构**同步复制/移动到输出目录，确保完整归档。
*   **进度可视化**：命令行界面提供实时进度条，显示摘要计算、比对和归档进度。
*   **报告输出**：
    *   `report.json`：完整的执行结果统计与明细。
    *   `duplicates.csv`：重复视频清单，方便人工核对。
    *   `process.log`：执行日志。
*   **AI 环境识别（可选）**：
    *   抽取 720p 单帧并调用本地 Ollama 模型识别环境类型。
    *   识别结果用于在省/市目录下增加环境子目录。
    *   置信度不足或识别失败则回落原目录结构。

## 安装与使用

### 前置要求

*   Go 1.20+ (如果需要源码编译)
*   已安装 ffmpeg（用于抽取 720p 单帧）
*   本地 Ollama 服务可用（用于环境识别）

### 编译运行

1.  克隆或下载本项目。
2.  在项目根目录下运行：

```bash
go run . -inputDir "D:\你的输入视频目录" -outputDir "D:\你的输出目录" [可选参数]
```

### 两步执行模式

1.  **生成规划文件（JSON/CSV）**

```bash
go run . -inputDir "D:\你的输入视频目录" -outputDir "D:\你的输出目录" -step plan
```

执行后会生成：

- `move_plan.json`
- `move_plan.csv`
- `move_plan.csv` 包含环境识别字段（environment/confidence/frame_path）

程序会提示是否继续移动文件，用户在命令行输入 yes/no 后才执行移动/复制。

2.  **根据规划文件执行移动**

```bash
go run . -inputDir "D:\你的输入视频目录" -outputDir "D:\你的输出目录" -step move
```

### 命令行参数

| 参数名 | 类型 | 默认值 | 说明 |
| :--- | :--- | :--- | :--- |
| `-inputDir` | string | (必填) | 待处理视频的根目录路径。 |
| `-outputDir` | string | (必填) | 结果输出目录路径。 |
| `-workers` | int | 4 | 并发处理的工作线程数。 |
| `-nearThreshold` | float | 0.8 | 近重复判定的相似度阈值 (0.0 - 1.0)。 |
| `-maxSizeDiffRatio` | float | 0.2 | 近重复判定允许的最大文件大小差异比例 (0.0 - 1.0)。 |
| `-copyMode` | bool | true | `true` 为复制文件（保留源文件），`false` 为移动文件。 |
| `-keepDuplicateCopies`| bool | false | 是否将重复的副本也保存到输出目录下的 `_duplicates` 文件夹中。 |
| `-step` | string | plan | 执行步骤：`plan` 生成规划文件，`move` 执行移动。 |
| `-planJson` | string | outputDir/move_plan.json | 规划 JSON 文件路径。 |
| `-planCsv` | string | outputDir/move_plan.csv | 规划 CSV 文件路径。 |
| `-confirmMove` | bool | true | 生成规划后是否等待确认继续移动。 |
| `-enableAI` | bool | false | 是否启用 AI 环境识别。 |
| `-frameDir` | string | outputDir/_frames | 抽帧输出目录。 |
| `-promptPath` | string | ./configs/prompts/video_frame_recognition_prompt.md | AI 识别提示词路径。 |
| `-envMinConfidence` | float | 0.6 | 环境识别置信度阈值，低于阈值则回落原目录。 |
| `-unknownProvince` | string | "待分类" | (内部逻辑用) 未知省份标识，触发原目录回落。 |
| `-unknownCity` | string | "未知城市" | (内部逻辑用) 未知城市标识，触发原目录回落。 |

### 示例

```bash
# 复制模式，并发数 8，相似度阈值 0.9
go run . -inputDir "D:\Data\Videos" -outputDir "D:\Data\Cleaned" -workers 8 -nearThreshold 0.9 -copyMode true
```

```bash
# 启用 AI 环境识别
go run . -inputDir "D:\Data\Videos" -outputDir "D:\Data\Cleaned" -step plan -enableAI true
```

## 项目结构

```text
internal/
├── app/        业务编排（流程入口）
├── config/     CLI 参数解析与校验
├── console/    Windows 控制台编码处理
├── scan/       文件扫描（视频/非视频）
├── hash/       哈希与抽样摘要
├── dedupe/     去重与相似度判断
├── ai/         抽帧与 Ollama 识别
├── plan/       规划文件与搬运执行
├── report/     报告输出
├── progress/   进度条
├── geo/        省市解析与归一化
└── model/      结构体定义
```

## 输出结构

输出目录结构示例：

```text
outputDir/
├── 浙江省/
│   └── 杭州市/
│       ├── 雪山/
│       │   └── video1.mp4
│       └── video2.mp4
├── 原始目录结构/
│   └── ... (省市无法识别或环境识别失败时回落)
├── _duplicates/
│   └── ... (若启用保留重复副本)
├── _frames/
│   └── ... (AI 抽帧输出)
├── move_plan.json
├── move_plan.csv
├── report.json
├── duplicates.csv
└── process.log
```

## 注意事项

1.  **备份数据**：虽然工具经过测试，但在处理重要数据（尤其是使用 `-copyMode=false` 移动模式）前，建议先备份原始数据。
2.  **非视频文件**：非视频文件会直接按原路径结构搬运，不进行去重。
3.  **同名文件**：输出目录若存在同名文件，工具会自动重命名（追加 `_1`, `_2` 等后缀）以避免覆盖。
