package ceui.lisa.core;

import ceui.lisa.database.AppDatabase;
import ceui.lisa.database.DownloadDao;
import ceui.lisa.database.DownloadingEntity;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.UUID;

/** Compatibility adapter for the existing pending-page table. All calls run on queue IO.
 * Completed downloads, duplicate-file probes and their Room schema remain untouched.
 */
final class PageQueuePersistence {
    private PageQueuePersistence() {}

    static String key(DownloadItem item) {
        return item.getName().length() + ":" + item.getName() + item.getUrl();
    }

    static String snapshot(Gson gson, DownloadItem item, boolean paused, int state) {
        JsonObject json = gson.toJsonTree(item).getAsJsonObject();
        json.addProperty("queueRecordVersion", 1);
        json.addProperty("paused", paused);
        // Running is a process-local attempt, never a reason to resume a phantom worker.
        json.addProperty("state", state == DownloadItem.DownloadState.FAILED
                ? DownloadItem.DownloadState.FAILED : DownloadItem.DownloadState.INIT);
        return gson.toJson(json);
    }

    static void admit(AppDatabase database, Gson gson, DownloadItem item) {
        if (item.getName() == null || item.getName().isEmpty() || item.getUrl() == null
                || item.getUrl().isEmpty() || item.getUuid() == null || item.getUuid().isEmpty()) {
            throw new IllegalArgumentException("Incomplete page queue identity");
        }
        DownloadingEntity entity = new DownloadingEntity();
        entity.setFileName(item.getName());
        entity.setUuid(item.getUuid());
        entity.setTaskGson(snapshot(gson, item, item.isPaused(), item.getState()));
        database.runInTransaction(() -> {
            DownloadDao dao = database.downloadDao();
            DownloadingEntity old = dao.getDownloadingByFileName(item.getName());
            if (old != null) {
                JsonObject previous = JsonParser.parseString(old.getTaskGson()).getAsJsonObject();
                // Legacy filename PK cannot represent different resources sharing one name.
                // Report the conflict; never silently replace an unrelated pending intention.
                if (!previous.has("url") || previous.get("url").isJsonNull()
                        || !Objects.equals(previous.get("url").getAsString(), item.getUrl())
                        || !previous.has("illust") || !previous.get("illust").isJsonObject()
                        || !previous.getAsJsonObject("illust").has("id")
                        || previous.getAsJsonObject("illust").get("id").getAsLong() != item.getIllust().getId()
                        || (previous.has("index") ? previous.get("index").getAsInt() : 0) != item.getIndex()) {
                    throw new IllegalStateException("Pending download filename conflict: " + item.getName());
                }
            }
            dao.insertDownloading(entity);
        });
    }

    /** Repair only the row that was read, never a newer replacement racing recovery. */
    static void restoreIdentity(DownloadDao dao, Gson gson, DownloadItem item,
                                String fileName, String columnUuid, String originalJson) {
        item.setName(fileName);
        String identity = columnUuid;
        if (identity == null || identity.isEmpty()) identity = item.getUuid();
        if (identity == null || identity.isEmpty()) {
            identity = UUID.nameUUIDFromBytes(("legacy-page:" + key(item))
                    .getBytes(StandardCharsets.UTF_8)).toString();
        }
        item.setUuid(identity);
        if (!Objects.equals(columnUuid, identity)) {
            dao.repairDownloadingIdentity(fileName, columnUuid, originalJson, identity, gson.toJson(item));
        }
    }
}
