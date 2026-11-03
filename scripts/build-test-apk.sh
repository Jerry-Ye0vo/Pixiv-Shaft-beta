#!/usr/bin/env bash
# Incremental debug build. Run with bash from any working directory.
set -euo pipefail

repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
abi="arm64-v8a"
output_dir="$repo_root/dist/test-apk"
run_tests=false
install_apk=false
offline=false

usage() {
    cat <<'USAGE'
Usage: bash scripts/build-test-apk.sh [options]
  --test          Run page-selection unit tests before packaging
  --install       Install the resulting APK on the single connected adb device
  --abi ABI       arm64-v8a (default) or x86_64 (emulator)
  --output DIR    APK/checksum destination (default: dist/test-apk)
  --offline       Use only previously cached Gradle dependencies
  -h, --help      Show help

Requires JDK 17+ and an Android SDK (ANDROID_HOME, ANDROID_SDK_ROOT,
or local.properties). First build downloads the Gradle toolchain, SDK/native
components and dependencies; later builds reuse Gradle/build caches.
The GitHub debug variant installs as Shaft(Debug), alongside the release app.
USAGE
}

while (($#)); do
    case "$1" in
        --test) run_tests=true; shift ;;
        --install) install_apk=true; shift ;;
        --offline) offline=true; shift ;;
        --abi|--output)
            if (($# < 2)); then echo "Missing value for $1" >&2; exit 2; fi
            if [[ "$1" == --abi ]]; then abi="$2"; else output_dir="$2"; fi
            shift 2 ;;
        -h|--help) usage; exit 0 ;;
        *) echo "Unknown option: $1" >&2; usage >&2; exit 2 ;;
    esac
done
case "$abi" in arm64-v8a|x86_64) ;; *) echo "Unsupported ABI: $abi" >&2; exit 2 ;; esac
if [[ "$output_dir" != /* ]]; then output_dir="$PWD/$output_dir"; fi
command -v java >/dev/null || { echo "Install JDK 17+ and configure JAVA_HOME." >&2; exit 1; }
if [[ ! -f "$repo_root/gradlew" ]]; then
    echo "Gradle wrapper is missing; use a complete repository checkout." >&2; exit 1
fi
if [[ ! -f "$repo_root/local.properties" && -z "${ANDROID_HOME:-}" && -z "${ANDROID_SDK_ROOT:-}" ]]; then
    echo "Set ANDROID_HOME/ANDROID_SDK_ROOT or sdk.dir in local.properties." >&2; exit 1
fi
adb_bin="${ANDROID_HOME:-${ANDROID_SDK_ROOT:-}}/platform-tools/adb"
if ! [[ -x "$adb_bin" ]]; then adb_bin="adb"; fi
if $install_apk; then
    command -v "$adb_bin" >/dev/null || { echo "adb was not found." >&2; exit 1; }
    "$adb_bin" get-state >/dev/null
fi

gradle_args=(--parallel --build-cache --console=plain "-PSHAFT_TEST_ABI=$abi")
if $offline; then gradle_args+=(--offline); fi
cd "$repo_root"
if $run_tests; then
    bash ./gradlew :app:testGithubDebugUnitTest \
        --tests ceui.pixiv.ui.download.PageDownloadSelectionTest "${gradle_args[@]}"
fi
bash ./gradlew :app:assembleGithubDebug "${gradle_args[@]}"

shopt -s nullglob
apks=("$repo_root"/app/build/outputs/apk/github/debug/*.apk)
if ((${#apks[@]} != 1)); then
    echo "Expected one universal debug APK, found ${#apks[@]}. Check Gradle outputs." >&2
    exit 1
fi
revision="$(git -C "$repo_root" rev-parse --short HEAD 2>/dev/null || printf local)"
mkdir -p "$output_dir"
artifact="$output_dir/pixiv-shaft-beta-$revision-$abi-debug.apk"
cp "${apks[0]}" "$artifact"
if command -v sha256sum >/dev/null; then
    sha256sum "$artifact" > "$artifact.sha256"
else
    shasum -a 256 "$artifact" > "$artifact.sha256"
fi
printf 'APK: %s\n' "$artifact"
if $install_apk; then "$adb_bin" install -r "$artifact"; fi
