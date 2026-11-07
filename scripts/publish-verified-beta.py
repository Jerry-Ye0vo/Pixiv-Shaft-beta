#!/usr/bin/env python3
"""Publish the verified Beta only after the exact source commit passes CI."""
import json
import os
from pathlib import Path
import subprocess
import time
import xml.etree.ElementTree as ET
import zipfile

def gh(*args):
    return subprocess.check_output(["gh", *args], text=True)

repo = os.environ["GITHUB_REPOSITORY"]
sha = os.environ["GITHUB_SHA"]
revision = int(os.environ["BETA_REVISION"])
tag = f"v4.9.9-beta.{revision}"
api = f"repos/{repo}"
deadline = time.monotonic() + 4200
gate = None
while time.monotonic() < deadline:
    runs = json.loads(gh("api", f"{api}/actions/runs?head_sha={sha}&per_page=100"))["workflow_runs"]
    candidates = [r for r in runs if r["path"] == ".github/workflows/gradle.yml" and r["head_sha"] == sha]
    if candidates:
        gate = max(candidates, key=lambda r: r["id"])
        if gate["status"] == "completed":
            if gate["conclusion"] != "success":
                raise SystemExit(f"CI failed; publication stopped: {gate['html_url']}")
            break
    print("Waiting for the exact source commit CI, including API 35/R8.", flush=True)
    time.sleep(30)
else:
    raise SystemExit("Timed out waiting for CI; APK retained as an Actions artifact.")
jobs = json.loads(gh("api", f"{api}/actions/runs/{gate['id']}/jobs?per_page=100"))["jobs"]
for name in ("Build apk", "Critical androidTest (API 35)"):
    if not any(j["name"] == name and j["conclusion"] == "success" for j in jobs):
        raise SystemExit(f"Required job did not pass: {name}")
selected_workflow = os.environ.get("BETA_REGRESSION_WORKFLOW", ".github/workflows/unified-queue-regression.yml")
selected_artifact = os.environ.get("BETA_REGRESSION_ARTIFACT", "unified-queue-regression")
selected = [r for r in runs if r["path"] == selected_workflow and r["head_sha"] == sha]
if not selected or max(selected, key=lambda r: r["id"])["conclusion"] != "success":
    raise SystemExit("Selected regression has not passed for the release source commit.")
selected = max(selected, key=lambda r: r["id"])
reports = Path("dist/validation")
reports.mkdir(parents=True, exist_ok=True)
artifacts = json.loads(gh("api", f"{api}/actions/runs/{gate['id']}/artifacts?per_page=100"))["artifacts"]
for prefix in ("unit-test-reports-", "lint-reports-", "android-test-reports-"):
    found = [a for a in artifacts if a["name"].startswith(prefix)]
    if len(found) != 1:
        raise SystemExit(f"Expected one report artifact: {prefix}")
    gh("run", "download", str(gate["id"]), "--repo", repo, "--name", found[0]["name"], "--dir", str(reports / prefix.rstrip("-")))
gh("run", "download", str(selected["id"]), "--repo", repo, "--name", selected_artifact, "--dir", str(reports / "selected-regression"))

def totals(paths):
    counts = dict(tests=0, failures=0, errors=0, skipped=0)
    suites = 0
    for path in paths:
        root = ET.parse(path).getroot()
        nodes = [root] if root.tag == "testsuite" else list(root.iter("testsuite"))
        for node in nodes:
            suites += 1
            for key in counts:
                counts[key] += int(node.attrib.get(key, "0"))
    return suites, counts

units = totals(sorted(reports.glob("unit-test-reports/**/test-results/**/TEST-*.xml")))
focused = totals(sorted(reports.glob("selected-regression/**/test-results/**/TEST-*.xml")))
android_paths = sorted(reports.glob("android-test-reports/**/outputs/androidTest-results/**/*.xml"))
android = totals(android_paths)
lint_paths = sorted(reports.glob("lint-reports/**/lint-results-*.xml"))
lint_errors = sum(1 for path in lint_paths for issue in ET.parse(path).getroot().findall("issue") if issue.attrib.get("severity") in ("Error", "Fatal"))
if not units[1]["tests"] or not focused[1]["tests"] or not android[1]["tests"] or len(lint_paths) < 10 or lint_errors:
    raise SystemExit("Missing test/lint reports or lint errors; publication stopped.")
if any(row[1]["failures"] or row[1]["errors"] for row in (units, focused, android)):
    raise SystemExit("JUnit reports contain failures; publication stopped.")
info_path = Path("dist/test-apk/build-info.txt")
info = dict(line.split("=", 1) for line in info_path.read_text().splitlines() if "=" in line)
if info["commit"] != sha or info["applicationId"] != "ceui.pixiv.pshaft.beta" or info["versionName"] != f"4.9.9-beta.{revision}" or int(info["versionCode"]) != 43600 + revision:
    raise SystemExit("Build identity mismatch.")
if info["certificateSha256"].lower() != Path("config/beta-signing-cert.sha256").read_text().strip().lower():
    raise SystemExit("Signing certificate mismatch.")
report = f"""# Beta {revision} 发布验证

- 源码提交：{sha}
- 包名：{info['applicationId']}
- 版本：{info['versionName']} / {info['versionCode']}
- 固定签名 SHA-256：{info['certificateSha256']}
- 全量 CI（含 API 35 与 minified Release/R8）：{gate['html_url']}
- 本地统计与离线打标专项回归：{selected['html_url']}

| 实际执行项 | 测试/报告数 | 失败 | 错误 | 跳过 |
| --- | ---: | ---: | ---: | ---: |
| app 与模块单元测试（{units[0]} 套件） | {units[1]['tests']} | {units[1]['failures']} | {units[1]['errors']} | {units[1]['skipped']} |
| 专项回归（{focused[0]} 套件，包含在全量测试中） | {focused[1]['tests']} | {focused[1]['failures']} | {focused[1]['errors']} | {focused[1]['skipped']} |
| API 35 仪器测试（{android[0]} 套件） | {android[1]['tests']} | {android[1]['failures']} | {android[1]['errors']} | {android[1]['skipped']} |
| lint XML | {len(lint_paths)} | {lint_errors} | 0 | 0 |

Room schema 提交完整性与现有数据库迁移、R8/Gson/native signer/search 契约检查均由上述 CI 门禁执行。专项覆盖作品去重统计、画像 DAO、预处理与阈值、打标队列冷恢复/暂停/固定边界/缓存复用、模型安装兼容和账号清理；界面测试含浅色窄屏、深色宽屏与字体放大。专项数与全量数有重叠，不相加。完整 XML/HTML 在 validation-reports.zip。

未执行：K80 Ultra 实机安装与长时间后台、慢网/断网实测、第三方 SAF 异常、满盘、原 ANR 场景实机复现。未提供这些项目的性能结论。

本次实现、模型来源、空间与速度验收方法见 local-profile-and-tagging.md；下载设置接线和此前统一管理器迁移差异见 unified-download-settings.md 与 unified-download-release-comparison.md。APK 的独立 .sha256 文件用于下载完整性校验。
"""
report_path = Path("dist/test-apk/validation-report.md")
report_path.write_text(report)
with zipfile.ZipFile("dist/test-apk/validation-reports.zip", "w", zipfile.ZIP_DEFLATED) as archive:
    for path in sorted(reports.rglob("*")):
        if path.is_file():
            archive.write(path, path.relative_to(reports))
for name in ("local-profile-and-tagging.md", "bookmark-and-download-lookup.md", "unified-download-settings.md", "unified-download-release-comparison.md"):
    Path("dist/test-apk", name).write_bytes(Path("docs", name).read_bytes())
notes = Path("dist/test-apk/release-notes.md")
notes.write_text(f"""本地统计与可选离线打标 Beta {revision}

延续固定 Beta 签名，包名含 .beta，可覆盖更新。新增侧菜单「统计 / 用户画像」，分析本机下载与当前账号本地收藏；可选下载约 106 MiB 的离线 INT8 模型，复用统一队列逐张打标、暂停续跑和相同内容缓存。数据库 46→47 仅新增分析缓存，保留旧记录、URI、JSON、查重和多页下载。每分钟超过 10 张需 K80 Ultra 实机验证，本次不宣称达标。

{report}

Beta 16 留档：https://github.com/{repo}/releases/tag/v4.9.9-beta.16
""")
assets = [str(p) for p in sorted(Path("dist/test-apk").iterdir()) if p.is_file() and p != notes]
subprocess.run(["gh", "release", "create", tag, "--repo", repo, "--target", sha, "--title", f"4.9.9-beta.{revision} · 本地统计与可选离线打标", "--prerelease", "--latest=false", "--notes-file", str(notes), *assets], check=True)
print(f"Published https://github.com/{repo}/releases/tag/{tag}")
