# Beta 固定签名与版本迭代

安装包身份为 `ceui.pixiv.pshaft.beta`。快速构建必须使用固定 keystore，缺少签名材料时停止，不再自动生成随机 Debug 签名。

## 一次性配置

在仓库 Settings → Secrets and variables → Actions → New repository secret 中配置：

| Secret | 对应材料 |
| --- | --- |
| `SHAFT_KEYSTORE_B64` | keystore 文件的 Base64 |
| `SHAFT_KEYSTORE_PASSWORD` | keystore 密码 |
| `SHAFT_KEY_ALIAS` | key alias |
| `SHAFT_KEY_PASSWORD` | key 密码 |

现已生成一份新的 Beta 专用 keystore，通过会话内的独立私密下载包交付。解压后四个 `SHAFT_*.txt` 文件分别对应上述 Secret，文件名去掉 `.txt` 为名称，文件内容为值。私钥和密码不放进 Git、不上传为工作流公开产物；保留备份，之后所有迭代复用同一份。

仓库仅记录公钥证书指纹 `config/beta-signing-cert.sha256`。工作流在构建前核对提供的 keystore，打包后再次核对 APK 证书和包名，避免换签名后误发。产物中的 `build-info.txt` 包含包名、版本名、版本号、证书指纹和提交号，不包含私钥。

如果尚未建立签名身份，可用 `bash scripts/prepare-beta-signing.sh` 一次性生成；不要对已有身份重复生成。本次已生成的身份应直接使用交付文件，重新生成会与记录的公钥指纹不同。

`SHAFT_EVENTS_HMAC` 是服务端接口签名密钥，与 Android 安装签名不同；它为空不会使 APK 变为无签名安装包。

## 版本迭代

`gradle.properties` 中的 `SHAFT_BETA_REVISION=1` 对应 `4.9.9-beta.1 / 43601`。发布下一轮时将该值递增；版本名和 Android versionCode 一起递增，保持安装包名和私钥不变。

本地构建可继续使用未跟踪的 `keystore.properties` 或 `SHAFT_KEYSTORE_*` 环境变量。快速脚本显式启用固定签名校验；常规单元测试不受此出包要求影响。

使用旧版临时 Debug 私钥签名的安装包，不能直接用这份新私钥覆盖。旧私钥未保留时，本次建立的新身份只能保证从此之后的版本保持一致；先通过应用提供的备份功能保留需要迁移的数据。
