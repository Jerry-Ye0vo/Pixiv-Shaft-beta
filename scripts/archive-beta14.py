"""Archive the exact previously tested Beta 14 artifact; never rebuild or re-sign."""
import hashlib
import json
import pathlib
import subprocess
import tempfile
import zipfile

REPO = "Jerry-Ye0vo/Pixiv-Shaft-beta"
COMMIT = "79eea653e7f2e4d7723dcd1847676fd74f90e0e6"
ARTIFACT = 13299260117
DIGEST = "2a120595c5c780e2bbe5ccf6c0d6e64823d04d2bb4bc6306c0136e15b0e10bae"
TAG = "v4.9.9-beta.14"
CERT = "98ff2935b059672b8f9ea236120cd1f2a55b08bbc5de85cd6ddc296ac1b7be8a"

def gh(*args):
    return subprocess.check_output(["gh", *args], text=True)

def main():
    with tempfile.TemporaryDirectory() as tmp:
        root = pathlib.Path(tmp)
        archive = root / "artifact.zip"
        with archive.open("wb") as out:
            subprocess.run(["gh", "api", f"repos/{REPO}/actions/artifacts/{ARTIFACT}/zip"], stdout=out, check=True)
        if hashlib.sha256(archive.read_bytes()).hexdigest() != DIGEST:
            raise RuntimeError("Artifact archive SHA-256 mismatch")
        files = root / "verified"
        files.mkdir()
        with zipfile.ZipFile(archive) as z:
            for item in z.infolist():
                dest = (files / item.filename).resolve()
                if not dest.is_relative_to(files.resolve()):
                    raise RuntimeError("Invalid archive path")
            z.extractall(files)
        info_files = list(files.rglob("build-info.txt"))
        assert len(info_files) == 1, "Expected one build-info.txt"
        info = dict(line.split("=", 1) for line in info_files[0].read_text().splitlines() if "=" in line)
        expected = dict(applicationId="ceui.pixiv.pshaft.beta", versionName="4.9.9-beta.14",
                        versionCode="43614", certificateSha256=CERT, commit=COMMIT, iteration="14")
        for key, value in expected.items():
            assert info.get(key) == value, f"Build metadata mismatch: {key}"
        apks = list(files.rglob("*.apk"))
        assert len(apks) == 1, "Expected one APK"
        apk = apks[0]
        checksum = pathlib.Path(str(apk) + ".sha256")
        assert checksum.is_file(), "Missing APK checksum"
        assert checksum.read_text().split()[0] == hashlib.sha256(apk.read_bytes()).hexdigest()
        notes = root / "notes.md"
        notes.write_text(
            "统一下载管理器重构前的 Beta 基线归档。\n\n"
            "- 版本：4.9.9-beta.14（43614），ARM64 Debug 测试包。\n"
            "- 包名：ceui.pixiv.pshaft.beta；沿用固定 Beta 签名。\n"
            f"- 源码：{COMMIT}\n"
            "- 原构建：https://github.com/Jerry-Ye0vo/Pixiv-Shaft-beta/actions/runs/41779429718\n"
            "- 直接保存原构建产物，未重新构建或签名。\n\n"
            "已知问题：日志出现手动继续时并发超限，以及大图浏览附近进程退出；"
            "本归档用于留档和回归对照，不包含后续统一调度改造。\n", encoding="utf-8")
        result = subprocess.run(["gh", "release", "view", TAG, "--repo", REPO,
                                 "--json", "isDraft"], capture_output=True, text=True)
        if result.returncode == 0 and not json.loads(result.stdout)["isDraft"]:
            print("Archive release already published; leaving it unchanged.")
            return
        if result.returncode != 0:
            gh("release", "create", TAG, "--repo", REPO, "--target", COMMIT,
               "--draft", "--prerelease", "--title", "4.9.9-beta.14 — 重构前留档",
               "--notes-file", str(notes))
        gh("release", "upload", TAG, str(apk), str(checksum), str(info_files[0]),
           "--repo", REPO, "--clobber")
        gh("release", "edit", TAG, "--repo", REPO, "--draft=false", "--prerelease", "--latest=false")
        print(f"https://github.com/{REPO}/releases/tag/{TAG}")

if __name__ == "__main__":
    main()
