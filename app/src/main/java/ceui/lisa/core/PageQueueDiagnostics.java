package ceui.lisa.core;

import android.app.ActivityManager;
import android.content.Context;
import android.os.Build;
import android.os.Process;
import ceui.lisa.BuildConfig;
import timber.log.Timber;

/** Bounded lifecycle diagnostics, with no resource URL or per-byte logging. */
final class PageQueueDiagnostics {
    private PageQueueDiagnostics() {}
    static void event(String event, DownloadItem item) {
        Timber.tag("ContentQueue").i("page event=%s pid=%d task=%s illust=%d page=%d state=%d paused=%s",
                event, Process.myPid(), item.getUuid(), item.getIllust().getId(),
                item.getIndex(), item.getState(), item.isPaused());
    }
    static void failure(String phase, DownloadItem item, Throwable error) {
        Timber.tag("ContentQueue").e(error, "page failure phase=%s pid=%d task=%s illust=%d page=%d",
                phase, Process.myPid(), item.getUuid(), item.getIllust().getId(), item.getIndex());
    }
    /** Called on IO. Exit reason codes are ApplicationExitInfo.REASON_*, memory is in kB. */
    static void previousExits(Context context) {
        if (Build.VERSION.SDK_INT < Build.VERSION_CODES.R) return;
        try {
            ActivityManager manager = (ActivityManager) context.getSystemService(Context.ACTIVITY_SERVICE);
            if (manager == null) return;
            Timber.tag("ContentQueue").i("page process pid=%d version=%s code=%d", Process.myPid(),
                    BuildConfig.VERSION_NAME, BuildConfig.VERSION_CODE);
            for (android.app.ApplicationExitInfo exit : manager.getHistoricalProcessExitReasons(context.getPackageName(), 0, 3)) {
                Timber.tag("ContentQueue").i("previous-exit pid=%d time=%d reason=%d status=%d importance=%d pssKB=%d rssKB=%d",
                        exit.getPid(), exit.getTimestamp(), exit.getReason(), exit.getStatus(),
                        exit.getImportance(), exit.getPss(), exit.getRss());
            }
        } catch (Exception error) {
            Timber.tag("ContentQueue").w(error, "Cannot read previous process exits");
        }
    }
}
