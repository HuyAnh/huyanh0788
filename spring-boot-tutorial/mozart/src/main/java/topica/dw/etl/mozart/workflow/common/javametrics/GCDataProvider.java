package topica.dw.etl.mozart.workflow.common.javametrics;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.Iterator;
import java.util.List;

/**
 * Uses MXBeans to get GC information
 */
public class GCDataProvider {

    private static long previousCollectionTime = 0;
    private static long previousRequestTimeStamp = 0;

    /**
     * Returns the time spent in GC as a proportion of the time elapsed since
     * this method was last called. If no data is available -1 is returned.
     * <p>
     * (Will always return -1 on first call.)
     *
     * @return
     */
    public static double getLatestGCPercentage() {
        long now = System.currentTimeMillis();
        long totalCollectionTime = getTotalCollectionTime();
        if (totalCollectionTime == -1) {
            return -1;
        }
        if (previousRequestTimeStamp == 0) {
            previousRequestTimeStamp = now;
            previousCollectionTime = totalCollectionTime;
            return -1;
        } else {
            long collectionTime = totalCollectionTime - previousCollectionTime;
            long elapsedTime = now - previousRequestTimeStamp;
            if (elapsedTime == 0) {
                return 0;
            }
            double timeInGc = (double) collectionTime / (double) elapsedTime;
            previousCollectionTime = totalCollectionTime;
            previousRequestTimeStamp = now;
            return timeInGc;
        }
    }

    /**
     * Returns the time spent in GC as a proportion of the time elapsed since
     * the JVM was started. If no data is available returns -1.
     *
     * @return the percentage of uptime spent in gc or -1.0
     */
    public static double getTotalGCPercentage() {
        long totalCollectionTime = getTotalCollectionTime();
        if (totalCollectionTime == -1) {
            return -1.0;
        }
        long uptime = ManagementFactory.getRuntimeMXBean().getUptime();
        return ((double) totalCollectionTime / (double) uptime);
    }

    private static long getTotalCollectionTime() {
        List<GarbageCollectorMXBean> sunBeans = ManagementFactory.getGarbageCollectorMXBeans();
        long totalCollectionTime = 0;
        if (sunBeans.size() == 0) {
            return -1;
        }
        for (Iterator<GarbageCollectorMXBean> iterator = sunBeans.iterator(); iterator.hasNext(); ) {
            GarbageCollectorMXBean garbageCollectorMXBean = iterator.next();
            long collectionTime = garbageCollectorMXBean.getCollectionTime();
            if (collectionTime != -1) {
                totalCollectionTime += collectionTime;
            }
        }
        return totalCollectionTime;
    }

}
