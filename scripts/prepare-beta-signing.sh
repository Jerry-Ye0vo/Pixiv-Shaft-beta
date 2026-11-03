#!/usr/bin/env bash
# Generate once, store in Actions Secrets, then reuse. Never overwrite an existing key.
set -euo pipefail
umask 077
repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
output_dir="${1:-$repo_root/../pixiv-shaft-beta-signing}"
if [[ "$output_dir" != /* ]]; then output_dir="$PWD/$output_dir"; fi
for command_name in keytool openssl base64; do
    command -v "$command_name" >/dev/null || { echo "Required tool: $command_name" >&2; exit 1; }
done
if [[ -e "$output_dir" ]]; then
    echo "Output directory already exists. Reuse its key; do not regenerate a signing identity." >&2
    exit 1
fi
mkdir -p "$output_dir"
beta_password="$(openssl rand -hex 32)"
export BETA_SIGNING_PASSWORD="$beta_password"
keytool -genkeypair -noprompt -storetype PKCS12 \
    -keystore "$output_dir/beta-signing.p12" \
    -alias shaft-beta -keyalg RSA -keysize 3072 -validity 10000 \
    -dname 'CN=Pixiv Shaft Beta,OU=Development,O=Jerry-Ye0vo' \
    -storepass:env BETA_SIGNING_PASSWORD -keypass:env BETA_SIGNING_PASSWORD
base64 < "$output_dir/beta-signing.p12" | tr -d '\r\n' > "$output_dir/SHAFT_KEYSTORE_B64.txt"
printf '%s' "$beta_password" > "$output_dir/SHAFT_KEYSTORE_PASSWORD.txt"
printf '%s' "$beta_password" > "$output_dir/SHAFT_KEY_PASSWORD.txt"
printf '%s' 'shaft-beta' > "$output_dir/SHAFT_KEY_ALIAS.txt"
keytool -exportcert -rfc -keystore "$output_dir/beta-signing.p12" \
    -alias shaft-beta -storepass:env BETA_SIGNING_PASSWORD -file "$output_dir/beta-signing.crt"
openssl x509 -in "$output_dir/beta-signing.crt" -noout -fingerprint -sha256 \
    > "$output_dir/certificate-sha256.txt"
unset BETA_SIGNING_PASSWORD beta_password
cat > "$output_dir/README.txt" <<'README'
Keep this directory private and back it up. Generate this identity once only.
GitHub repository Settings > Secrets and variables > Actions > New repository secret:
Use each SHAFT_*.txt filename (without .txt) as the secret name and its contents as the value.
Do not commit these files or upload them as public workflow artifacts.
An older Beta signed with another key cannot be upgraded with this new identity.
README
printf 'Signing files created in: %s\n' "$output_dir"
cat "$output_dir/certificate-sha256.txt"
