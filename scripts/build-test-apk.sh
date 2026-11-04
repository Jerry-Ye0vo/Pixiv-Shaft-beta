#!/usr/bin/env bash
# Incremental debug build. Run with bash from any working directory.
set -euo pipefail

repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
abi="arm64-v8a"
output_dir="$repo_root/dist/test-apk"
run_tests=false
install_apk=false
offline=false
beta_revision="${SHAFT_BETA_REVISION:-}"

usage() {
    cat <<'USAGE'
Usage: bash scripts/build-test-apk.sh [options]
  --test          Run page-selection and unified-queue recovery tests before packaging
  --install       Install the resulting APK on the single connected adb device
  --abi ABI       arm64-v8a (default) or x86_64 (emulator)
  --output DIR    APK/checksum destination (default: dist/test-apk)
  --offline       Use only previously cached Gradle dependencies
  --revision N    Override the Beta iteration (1..9999)
  -h, --help      Show help

Requires JDK 17+ and an Android SDK (ANDROID_HOME, ANDROID_SDK_ROOT,
or local.properties). First build downloads the Gradle toolchain, SDK/native
components and dependencies; later builds reuse Gradle/build caches.
Installs as Shaft.beta (ceui.pixiv.pshaft.beta), alongside the original app.
A fixed signing key is required via keystore.properties or SHAFT_KEYSTORE_*.
CI uses its workflow run number. Local builds reserve the next iteration in
.beta-build-revision, starting at SHAFT_BETA_REVISION in gradle.properties.
When switching build sources, --revision can select a number above the installed version.
USAGE
}

while (($#)); do
    case "$1" in
        --test) run_tests=true; shift ;;
        --install) install_apk=true; shift ;;
        --offline) offline=true; shift ;;
        --abi|--output|--revision)
            if (($# < 2)); then echo "Missing value for $1" >&2; exit 2; fi
            case "$1" in
                --abi) abi="$2" ;;
                --output) output_dir="$2" ;;
                --revision) beta_revision="$2" ;;
            esac
            shift 2 ;;
        -h|--help) usage; exit 0 ;;
        *) echo "Unknown option: $1" >&2; usage >&2; exit 2 ;;
    esac
done
case "$abi" in arm64-v8a|x86_64) ;; *) echo "Unsupported ABI: $abi" >&2; exit 2 ;; esac
valid_revision() { [[ "$1" =~ ^[1-9][0-9]{0,3}$ ]]; }
if [[ -n "$beta_revision" ]] && ! valid_revision "$beta_revision"; then
    echo "Beta revision must be an integer from 1 to 9999." >&2; exit 2
fi
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

version_source="explicit"
if [[ -z "$beta_revision" && -n "${GITHUB_RUN_NUMBER:-}" ]]; then
    beta_revision="$GITHUB_RUN_NUMBER"
    version_source="ci"
elif [[ -z "$beta_revision" ]]; then
    version_source="local"
fi
if [[ "$version_source" == ci ]]; then
    valid_revision "$beta_revision" || { echo "Invalid CI Beta revision." >&2; exit 2; }
else
    revision_lock="$repo_root/.beta-build-revision.lock"
    if ! mkdir "$revision_lock" 2>/dev/null; then
        echo "Another local build owns $revision_lock. Remove a stale lock only after it has stopped." >&2
        exit 1
    fi
    trap 'rmdir "$revision_lock" 2>/dev/null || true' EXIT
    previous_revision=0
    if [[ -f "$repo_root/.beta-build-revision" ]]; then
        previous_revision="$(< "$repo_root/.beta-build-revision")"
        valid_revision "$previous_revision" || { echo "Invalid local Beta counter." >&2; exit 2; }
    fi
    if [[ "$version_source" == local ]]; then
        minimum_revision=2
        if [[ -f "$repo_root/gradle.properties" ]]; then
            configured_revision="$(awk -F= '$1 ~ /^[[:space:]]*SHAFT_BETA_REVISION[[:space:]]*$/ {gsub(/[[:space:]]/, "", $2); print $2; exit}' "$repo_root/gradle.properties")"
            if [[ -n "$configured_revision" ]]; then minimum_revision="$configured_revision"; fi
        fi
        valid_revision "$minimum_revision" || { echo "Invalid configured Beta revision." >&2; exit 2; }
        beta_revision=$((previous_revision + 1))
        if ((beta_revision < minimum_revision)); then beta_revision="$minimum_revision"; fi
    fi
    valid_revision "$beta_revision" || { echo "Beta revision limit reached (9999)." >&2; exit 2; }
    next_counter="$beta_revision"
    if ((next_counter < previous_revision)); then next_counter="$previous_revision"; fi
    printf '%s\n' "$next_counter" > "$repo_root/.beta-build-revision.tmp.$$"
    mv "$repo_root/.beta-build-revision.tmp.$$" "$repo_root/.beta-build-revision"
    rmdir "$revision_lock"
    trap - EXIT
fi
printf 'Beta version: 4.9.9-beta.%s (%s)\n' "$beta_revision" "$version_source"

gradle_args=(--parallel --build-cache --console=plain "-PSHAFT_TEST_ABI=$abi"
    -PSHAFT_BETA_TEST=true -PSHAFT_REQUIRE_BETA_SIGNING=true "-PSHAFT_BETA_REVISION=$beta_revision")
if $offline; then gradle_args+=(--offline); fi
cd "$repo_root"
if $run_tests; then
    bash ./gradlew :app:testGithubDebugUnitTest \
        --tests ceui.pixiv.ui.download.PageDownloadSelectionTest \
        --tests ceui.pixiv.download.ImagePageSaveJournalTest \
        --tests ceui.pixiv.download.LegacyPageSaveMigrationTest \
        --tests ceui.pixiv.ui.detail.PageDownloadFabStateTest "${gradle_args[@]}"
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
artifact="$output_dir/pixiv-shaft-4.9.9-beta.$beta_revision-$revision-$abi-debug.apk"
cp "${apks[0]}" "$artifact"
(
    cd "$output_dir"
    if command -v sha256sum >/dev/null; then
        sha256sum "${artifact##*/}" > "${artifact##*/}.sha256"
    else
        shasum -a 256 "${artifact##*/}" > "${artifact##*/}.sha256"
    fi
)
printf 'APK: %s\n' "$artifact"
if $install_apk; then "$adb_bin" install -r "$artifact"; fi

