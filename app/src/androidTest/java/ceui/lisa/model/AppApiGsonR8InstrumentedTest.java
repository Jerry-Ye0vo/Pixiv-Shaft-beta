package ceui.lisa.model;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import androidx.test.ext.junit.runners.AndroidJUnit4;
import androidx.lifecycle.LiveData;

import com.google.gson.Gson;

import org.json.JSONObject;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.WildcardType;
import java.util.List;
import java.util.Map;

import ceui.lisa.core.DownloadItem;
import ceui.pixiv.download.copy.CopyIntent;
import ceui.pixiv.download.queue.ContentPhaseRunner;
import ceui.pixiv.api.API;
import ceui.pixiv.api.model.Illust;
import ceui.pixiv.cache.ObjectPool;
import ceui.pixiv.snapshot.SnapshotManifest;
import ceui.pixiv.snapshot.AutoSnapshotBehaviorRecord;
import ceui.pixiv.snapshot.AutoSnapshotDwellSample;
import ceui.pixiv.ui.user.RequestPlanText;
import ceui.pixiv.ui.user.UserRequestPlansResponse;
import ceui.pixiv.plaza.CreatePost;
import ceui.pixiv.plaza.DeletePost;
import ceui.pixiv.plaza.PlazaApi;
import ceui.pixiv.plaza.PlazaBlockedUser;
import ceui.pixiv.plaza.PlazaBlocks;
import ceui.pixiv.plaza.PlazaCommentPreview;
import ceui.pixiv.plaza.PlazaImage;
import ceui.pixiv.plaza.PlazaPage;
import ceui.pixiv.plaza.PlazaPost;
import ceui.pixiv.plaza.PlazaReaction;
import ceui.pixiv.plaza.PlazaReportReceipt;
import ceui.pixiv.plaza.PlazaReportRequest;
import ceui.pixiv.sticker.StickerCatalog;
import ceui.pixiv.sticker.StickerPack;
import ceui.pixiv.sticker.Sticker;
import retrofit2.http.GET;

/** Guards App API wire fields and Retrofit response signatures in the minified release APK. */
@RunWith(AndroidJUnit4.class)
public final class AppApiGsonR8InstrumentedTest {

    @Test
    public void ugoiraParentKeepsFrozenDestinationsAndActualFormatReceiptInRelease() throws Exception {
        Object request = parseWire(ceui.pixiv.ui.bulk.UgoiraSaveRequest.class,
                "{\"version\":1,\"illustJson\":\"{}\",\"preferVideo\":true,\"gif\":{\"storage\":\"cache\",\"segments\":[\"frozen.gif\"],\"mime\":\"image/gif\",\"policy\":\"Rename\"},\"video\":{\"storage\":\"cache\",\"segments\":[\"frozen.mp4\"],\"mime\":\"video/mp4\",\"policy\":\"Rename\"}}");
        JSONObject frozen = new JSONObject(encodeWire(request));
        assertTrue(frozen.getBoolean("preferVideo"));
        assertEquals("frozen.gif", frozen.getJSONObject("gif").getJSONArray("segments").getString(0));
        assertEquals("video/mp4", frozen.getJSONObject("video").getString("mime"));
        Object actual = parseWire(ceui.pixiv.ui.bulk.UgoiraSaveDomain.class,
                "{\"version\":1,\"video\":false,\"metadataJson\":\"frozen-metadata\"}");
        JSONObject receipt = new JSONObject(encodeWire(actual));
        assertFalse(receipt.getBoolean("video"));
        assertEquals("frozen-metadata", receipt.getString("metadataJson"));
    }

    @Test
    public void novelExportTokensAndArtifactReceiptsKeepTheirSchemaInRelease() throws Exception {
        Object request = parseWire(ceui.pixiv.ui.novel.reader.export.NovelExportRequest.class,
                "{\"version\":1,\"format\":\"Txt\",\"novelId\":123,\"tokens\":[{\"type\":\"paragraph\",\"start\":0,\"end\":4,\"text\":\"body\",\"id\":0,\"page\":0}],\"destination\":{\"storage\":\"cache\",\"segments\":[\"book.txt\"],\"mime\":\"text/plain\",\"policy\":\"Rename\"},\"header\":\"frozen\",\"legacyTxt\":false,\"retryMode\":\"auto\"}");
        JSONObject frozen = new JSONObject(encodeWire(request));
        assertEquals(123L, frozen.getLong("novelId"));
        assertEquals("paragraph", frozen.getJSONArray("tokens").getJSONObject(0).getString("type"));
        assertEquals("body", frozen.getJSONArray("tokens").getJSONObject(0).getString("text"));
        assertEquals("frozen", frozen.getString("header"));
        Object receipt = parseWire(ceui.pixiv.download.queue.ArtifactCheckpoint.class,
                "{\"version\":1,\"bytes\":123,\"sha256\":\"hash\",\"receipt\":\"content://output/1\",\"skipped\":false,\"outputCommitted\":true,\"domain\":\"checkpoint\"}");
        JSONObject saved = new JSONObject(encodeWire(receipt));
        assertEquals("content://output/1", saved.getString("receipt"));
        assertEquals("hash", saved.getString("sha256"));
        assertTrue(saved.getBoolean("outputCommitted"));
    }

    @Test
    public void modelParentAndInstallationReceiptsKeepTheirSchemaInRelease() throws Exception {
        Object request = parseWire(ceui.pixiv.download.model.ModelRequest.class,
                "{\"version\":1,\"directory\":\"rife-models\",\"asset\":\"rife\",\"required\":[\"model.onnx\"],\"operation\":\"download\",\"source\":\"https://example.com/model.zip\",\"timeoutSeconds\":120}");
        JSONObject frozen = new JSONObject(encodeWire(request));
        assertEquals("rife-models", frozen.getString("directory"));
        assertEquals("model.onnx", frozen.getJSONArray("required").getString(0));
        assertEquals(120L, frozen.getLong("timeoutSeconds"));
        Object receipt = parseWire(ceui.pixiv.download.model.ModelInstallReceipt.class,
                "{\"version\":1,\"plan\":\"plan:test\",\"request\":\"request-hash\",\"source\":\"source-hash\",\"files\":[{\"path\":\"model.onnx\",\"length\":123,\"sha256\":\"file-hash\"}]}");
        JSONObject saved = new JSONObject(encodeWire(receipt));
        assertEquals("plan:test", saved.getString("plan"));
        assertEquals(123L, saved.getJSONArray("files").getJSONObject(0).getLong("length"));
        assertEquals("file-hash", saved.getJSONArray("files").getJSONObject(0).getString("sha256"));
        Object checkpoint = parseWire(ceui.pixiv.download.model.ModelCheckpoint.class,
                "{\"version\":1,\"length\":123,\"sha256\":\"source-hash\",\"invalid\":true}");
        assertTrue(new JSONObject(encodeWire(checkpoint)).getBoolean("invalid"));
    }

    @Test
    public void unifiedDownloadV1InputsAndReceiptsKeepTheirPersistedSchemaInRelease() throws Exception {
        Object intent = parseWire(CopyIntent.class, """
                {"version":1,"request":{"kind":"translation","subjectId":123,"page":2,
                  "sourcePath":"/data/local/tmp/translated.png","sourceSize":123,"sourceModified":456,
                  "contextJson":"{}","submittedAt":789,"destination":{"storage":"cache",
                    "segments":["translated.png"],"mime":"image/png","policy":"Rename"}},
                  "snapshotName":"owned.source","sourceHash":"sha256","sourceBytes":123,
                  "receiptUri":"content://media/external/images/media/123","skipped":false,"outputCommitted":true}
                """);
        assertEquals(1, wireField(intent, "version"));
        Object request = wireField(intent, "request");
        assertEquals(123L, wireField(request, "subjectId"));
        assertEquals(2, wireField(request, "page"));
        Object destination = wireField(request, "destination");
        assertEquals("cache", wireField(destination, "storage"));
        assertEquals("Rename", wireField(destination, "policy"));
        assertEquals("translated.png", ((List<?>) wireField(destination, "segments")).get(0));
        JSONObject persisted = new JSONObject(encodeWire(intent));
        assertEquals("sha256", persisted.getString("sourceHash"));
        assertEquals(123L, persisted.getLong("sourceBytes"));
        assertTrue(persisted.getBoolean("outputCommitted"));
        assertEquals("image/png", persisted.getJSONObject("request").getJSONObject("destination").getString("mime"));
        Object restored = parseWire(CopyIntent.class, persisted.toString());
        assertEquals("content://media/external/images/media/123", wireField(restored, "receiptUri"));

        Object phase = parseWire(ContentPhaseRunner.Intent.class,
                "{\"version\":1,\"owner\":\"ugoira\",\"phase\":\"zip\",\"resource\":\"target\",\"context\":\"{}\",\"submittedAt\":42}");
        JSONObject phaseJson = new JSONObject(encodeWire(phase));
        assertEquals("ugoira", phaseJson.getString("owner"));
        assertEquals("zip", phaseJson.getString("phase"));
        assertEquals("target", phaseJson.getString("resource"));
        assertEquals(42L, phaseJson.getLong("submittedAt"));
    }

    @Test
    public void listIllustWireFieldsSurviveR8() throws Exception {
        // Gson reads these legacy fields by their source names. Checking the actual release class
        // avoids linking the test APK against Gson methods that target R8 is allowed to optimize.
        assertNotNull(ListIllust.class.getDeclaredField("next_url"));
        assertNotNull(ListIllust.class.getDeclaredField("illusts"));
        assertNotNull(Illust.class.getDeclaredField("id"));
        assertNotNull(Illust.class.getDeclaredField("title"));
    }

    @Test
    public void reflectionOnlyModelFieldsSurviveR8() throws Exception {
        // These models are only ever instantiated by Gson. Without an identity keep, full-mode R8
        // marks them uninstantiated and strips their fields even though -keepclassmembers lists
        // them. One representative per category: fully-qualified Retrofit response, nested wire
        // model, exported .shaftsnap manifest, and the Room-persisted download queue snapshot.
        assertNotNull(UserRequestPlansResponse.class.getDeclaredField("request_plans"));
        assertNotNull(RequestPlanText.class.getDeclaredField("translation"));
        assertNotNull(SnapshotManifest.class.getDeclaredField("snapshotId"));
        assertNotNull(DownloadItem.class.getDeclaredField("url"));
    }

    @Test
    public void autoSnapshotBehaviorSchemaSurvivesR8() throws Exception {
        for (String field : new String[]{"illustId", "type", "recentVisits", "visitCount",
                "lastDwellMs", "recentDwells", "lastAutoSnapshotAt", "lastTriggerSignal", "schemaVersion"}) {
            assertNotNull(AutoSnapshotBehaviorRecord.class.getDeclaredField(field));
        }
        assertNotNull(AutoSnapshotDwellSample.class.getDeclaredField("at"));
        assertNotNull(AutoSnapshotDwellSample.class.getDeclaredField("ms"));
    }

    @Test
    public void retrofitResponseGenericTypesSurviveR8FullMode() {
        // API is suspend-only: the response type lives in the trailing Continuation<? super T>
        // parameter, which Retrofit reads reflectively. Two endpoints from different call paths
        // (recommended feed via widgets, latest works via LatestIllustRepo).
        assertEquals(RecmdIllust.class, continuationResultType(findGetEndpoint(API.class, "v1/illust/recommended")));
        assertEquals(ListIllust.class, continuationResultType(findGetEndpoint(API.class, "v1/illust/new")));
    }

    @Test
    public void plazaEmptyAndPopulatedResponsesDeserializeInRelease() throws Exception {
        assertEquals(PlazaPage.class, continuationResultType(findGetEndpoint(PlazaApi.class, "v1/plaza/posts")));
        assertEquals(PlazaBlocks.class, continuationResultType(findGetEndpoint(PlazaApi.class, "v1/plaza/blocks")));
        Object empty = parseWire(PlazaPage.class, "{\"items\":[],\"nextBefore\":null}");
        assertTrue(((List<?>) wireField(empty, "items")).isEmpty());
        assertEquals(null, wireField(empty, "nextBefore"));

        Object page = parseWire(PlazaPage.class, """
                {"items":[{"id":14,"uid":42,"displayName":"author","text":"hello",
                  "createdAt":1700000000000,"objectId":123,"objectType":"illust","replyTo":null,
                  "likeCount":2,"replyCount":1,"liked":true,"title":"title","avatarUrl":"https://example.com/a",
                  "images":[{"mediaId":"media-1","width":80,"height":60,"contentType":"image/png",
                    "url":"https://example.com/image","expiresAt":1800000000000}],
                  "reactions":[{"emoji":"sticker:650863185465585230","count":3,"selected":true,"stickerId":650863185465585230}],
                  "commentsPreview":[{"id":15,"uid":43,"displayName":"reply","text":"comment","avatarUrl":null,"createdAt":1700000001000}],
                  "objectExtensions":{"illust":{"id":123,"title":"linked"}}}],"nextBefore":14}
                """);
        assertEquals(14L, wireField(page, "nextBefore"));
        Object post = ((List<?>) wireField(page, "items")).get(0);
        assertEquals(PlazaPost.class, post.getClass());
        assertEquals(14L, wireField(post, "id"));
        assertEquals("author", wireField(post, "displayName"));
        assertEquals("title", wireField(post, "title"));
        Object image = ((List<?>) wireField(post, "images")).get(0);
        assertEquals(PlazaImage.class, image.getClass());
        assertEquals("media-1", wireField(image, "mediaId"));
        assertEquals(80, wireField(image, "width"));
        Object reaction = ((List<?>) wireField(post, "reactions")).get(0);
        assertEquals(PlazaReaction.class, reaction.getClass());
        assertEquals(650863185465585230L, wireField(reaction, "stickerId"));
        Object reply = ((List<?>) wireField(post, "commentsPreview")).get(0);
        assertEquals(PlazaCommentPreview.class, reply.getClass());
        assertEquals("comment", wireField(reply, "text"));
        Object linked = wireField(wireField(post, "objectExtensions"), "illust");
        assertEquals(Illust.class, linked.getClass());
        assertEquals("linked", wireField(linked, "title"));
        // The same schema is persisted in the first-page cache and passed to the image viewer.
        Object restored = parseWire(PlazaPage.class, encodeWire(page));
        assertEquals(14L, wireField(((List<?>) wireField(restored, "items")).get(0), "id"));
    }

    @Test
    public void plazaRequestsAndModerationResponsesKeepTheirWireSchema() throws Exception {
        String createJson = """
                {"requestId":"request-id-123456","text":"hello","displayName":"author","mediaIds":["media-1"],
                 "objectId":123,"objectType":"illust","replyTo":14,"title":"title","avatarUrl":"https://example.com/a",
                 "policyVersion":"2026-09-16","objectExtensions":{"illust":{"id":123,"title":"linked"}}}
                """;
        JSONObject request = new JSONObject(encodeWire(parseWire(CreatePost.class, createJson)));
        assertEquals(11, request.length());
        assertEquals("request-id-123456", request.getString("requestId"));
        assertEquals("author", request.getString("displayName"));
        assertEquals("media-1", request.getJSONArray("mediaIds").getString(0));
        assertEquals(14L, request.getLong("replyTo"));
        assertEquals("2026-09-16", request.getString("policyVersion"));
        assertEquals(123L, request.getJSONObject("objectExtensions").getJSONObject("illust").getLong("id"));

        JSONObject report = new JSONObject(encodeWire(parseWire(PlazaReportRequest.class,
                "{\"targetType\":\"post\",\"reason\":\"spam\",\"details\":\"reason\",\"mediaIds\":[\"media-1\"]}")));
        assertEquals(4, report.length());
        assertEquals("post", report.getString("targetType"));
        assertEquals("spam", report.getString("reason"));
        Object receipt = parseWire(PlazaReportReceipt.class, "{\"id\":1,\"status\":\"pending\",\"duplicate\":false}");
        assertEquals(1L, wireField(receipt, "id"));
        assertEquals("pending", wireField(receipt, "status"));
        assertEquals(false, wireField(receipt, "duplicate"));
        assertEquals(true, wireField(parseWire(DeletePost.class, "{\"ok\":true}"), "ok"));
        Object blocks = parseWire(PlazaBlocks.class, "{\"items\":[{\"uid\":42,\"displayName\":\"blocked\"}]}");
        Object blocked = ((List<?>) wireField(blocks, "items")).get(0);
        assertEquals(PlazaBlockedUser.class, blocked.getClass());
        assertEquals("blocked", wireField(blocked, "displayName"));
    }

    @Test
    public void stickerCatalogAndNestedCollectionsSurviveRelease() throws Exception {
        Object catalog = parseWire(StickerCatalog.class, """
                {"versions":{"list":[{"name":"static","version":"v1"}]},"packs":{"static":{
                  "pkgList":[{"width":128,"height":128,"url":"https://example.com/stickers.zip",
                    "path":"emoji/128","size":1234,"sha256":"hash"}],
                  "groupList":[{"name":"test","stickerList":[{"stickerId":650863185465585230,"name":"smile",
                    "media":{"baseUrl":"https://example.com/","resourceList":[{"width":128,"height":128,"url":"smile.png"}]}}]}],
                  "stickerList":[]}}}
                """);
        Object version = ((List<?>) wireField(wireField(catalog, "versions"), "list")).get(0);
        assertEquals("v1", wireField(version, "version"));
        Object pack = ((Map<?, ?>) wireField(catalog, "packs")).get("static");
        assertEquals(StickerPack.class, pack.getClass());
        Object pkg = ((List<?>) wireField(pack, "pkgList")).get(0);
        assertEquals(1234L, wireField(pkg, "size"));
        Object group = ((List<?>) wireField(pack, "groupList")).get(0);
        Object sticker = ((List<?>) wireField(group, "stickerList")).get(0);
        assertEquals(Sticker.class, sticker.getClass());
        assertEquals(650863185465585230L, wireField(sticker, "stickerId"));
        Object media = wireField(sticker, "media");
        Object resource = ((List<?>) wireField(media, "resourceList")).get(0);
        assertEquals("smile.png", wireField(resource, "url"));
        JSONObject persisted = new JSONObject(encodeWire(catalog));
        assertEquals("v1", persisted.getJSONObject("versions").getJSONArray("list").getJSONObject(0).getString("version"));
    }

    // Reflect over the Gson shipped in the target APK: androidTest is shrunk separately and
    // cannot assume that Gson's production method names/ABI survive target R8 unchanged.
    private static Object parseWire(Class<?> type, String json) throws Exception {
        assertTrue("Gson response type became abstract: " + type, !Modifier.isAbstract(type.getModifiers()));
        Object gson = Gson.class.getDeclaredConstructor().newInstance();
        for (Method method : Gson.class.getDeclaredMethods()) {
            Class<?>[] params = method.getParameterTypes();
            if (params.length == 2 && params[0] == String.class && params[1] == Class.class) {
                method.setAccessible(true);
                return method.invoke(gson, json, type);
            }
        }
        throw new AssertionError("Missing Gson string/class parser");
    }

    private static String encodeWire(Object value) throws Exception {
        Object gson = Gson.class.getDeclaredConstructor().newInstance();
        for (Method method : Gson.class.getDeclaredMethods()) {
            Class<?>[] params = method.getParameterTypes();
            if (params.length == 1 && params[0] == Object.class && method.getReturnType() == String.class) {
                method.setAccessible(true);
                return (String) method.invoke(gson, value);
            }
        }
        throw new AssertionError("Missing Gson object serializer");
    }

    private static Object wireField(Object value, String name) throws Exception {
        Field field = value.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return field.get(value);
    }

    private static Type continuationResultType(Method suspendMethod) {
        Type[] params = suspendMethod.getGenericParameterTypes();
        ParameterizedType continuation = (ParameterizedType) params[params.length - 1];
        return unwrapWildcard(continuation.getActualTypeArguments()[0]);
    }

    @Test
    public void objectPoolTypeKeysDoNotDependOnObfuscatedClassNames() throws Exception {
        long id = 9_223_372_036_854_770_000L;

        // Target R8 may staticize ObjectPool's instance methods, while androidTest is optimized in
        // a separate APK. Invoke the actual release methods reflectively so this test validates
        // ObjectPool semantics without adding production keep rules just for the test ABI.
        //
        // 池里不止一个「Illust → void」入口（updateIllust / updateIllustTrusted / seedUntrusted），
        // 而 getDeclaredMethods() 的顺序是未定义的：seedUntrusted 存进去的是去掉 viewer 态之后的
        // 副本（withoutViewerState 在 is_bookmarked 非空时 copy），把它当「刚存进去的那份」断言
        // 同一实例必然失败。所以每个入口各用一个新 id 试一次，命中同一实例即通过 —— 与反射顺序无关。
        boolean updated = false;
        int slot = 0;
        for (Method method : ObjectPool.class.getDeclaredMethods()) {
            Class<?>[] parameters = method.getParameterTypes();
            if (parameters.length == 1 && parameters[0] == Illust.class
                    && method.getReturnType() == void.class) {
                // 每个入口各用一个新 id：同一个 id 连着写两次会走 mergeKeepingExisting，
                // 存进去的又是一个新实例，identity 断言就分不清「入口存了副本」和「池读不出来」。
                long probeId = id - slot++;
                Illust probe = newIllust(probeId);
                method.setAccessible(true);
                method.invoke(receiverFor(method), probe);
                updated = true;
                if (readsBackSameInstance(probeId, probe)) return;
            }
        }
        assertTrue("Missing ObjectPool Illust update method", updated);
        fail("Obfuscated ObjectPool could not read the Illust that it just stored");
    }

    private static Illust newIllust(long id) {
        return new Illust(
                null, null, 800, id, null, false, 0, false, null, null,
                1, null, null, null, null, "r8-object-pool", null, 10, 20,
                "illust", null, true, 1200, null, false, null);
    }

    /**
     * 读回 id 对应的 LiveData，看里面是不是刚写进去的那一份。新 key 是直接以对象建 LiveData
     *（不是 setValue），所以这里不需要等主线程；池里所有 (long) → LiveData 的读口都要试一遍。
     */
    private static boolean readsBackSameInstance(long id, Illust illust) throws Exception {
        for (Method method : ObjectPool.class.getDeclaredMethods()) {
            Class<?>[] parameters = method.getParameterTypes();
            if (parameters.length == 1 && parameters[0] == long.class
                    && LiveData.class.isAssignableFrom(method.getReturnType())) {
                method.setAccessible(true);
                LiveData<?> result = (LiveData<?>) method.invoke(receiverFor(method), id);
                if (result != null && result.getValue() == illust) {
                    assertSame(illust, result.getValue());
                    return true;
                }
            }
        }
        return false;
    }

    private static Object receiverFor(Method method) throws Exception {
        if (Modifier.isStatic(method.getModifiers())) {
            return null;
        }
        for (Field field : ObjectPool.class.getDeclaredFields()) {
            if (Modifier.isStatic(field.getModifiers()) && field.getType() == ObjectPool.class) {
                field.setAccessible(true);
                return field.get(null);
            }
        }
        throw new AssertionError("Missing ObjectPool singleton instance");
    }

    private static Method findGetEndpoint(Class<?> apiClass, String endpointPath) {
        for (Method method : apiClass.getDeclaredMethods()) {
            GET get = method.getAnnotation(GET.class);
            if (get != null && get.value().split("\\?", 2)[0].equals(endpointPath)) {
                return method;
            }
        }
        throw new AssertionError("Missing Retrofit endpoint: " + endpointPath);
    }

    private static Type unwrapWildcard(Type type) {
        if (!(type instanceof WildcardType)) {
            return type;
        }
        WildcardType wildcard = (WildcardType) type;
        Type[] lowerBounds = wildcard.getLowerBounds();
        return lowerBounds.length == 1 ? lowerBounds[0] : wildcard.getUpperBounds()[0];
    }
}

