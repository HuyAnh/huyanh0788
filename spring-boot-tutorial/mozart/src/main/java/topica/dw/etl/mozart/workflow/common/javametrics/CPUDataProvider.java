package topica.dw.etl.mozart.workflow.common.javametrics;

import java.lang.management.ManagementFactory;
import java.lang.reflect.Method;

/**
 * Uses MXBeans to get CPU statistics
 */
public class CPUDataProvider {

    /**
     * Get the system CPU usage
     */
    public static double getSystemCpuLoad() {
        return getCpuLoad(true);
    }

    /**
     * Get the Java process CPU usage
     */
    public static double getProcessCpuLoad() {
        return getCpuLoad(false);
    }

    private static double getCpuLoad(boolean system) {
        String cpuLoadMethod = system ? "getSystemCpuLoad" : "getProcessCpuLoad";
        Class<?> noparams[] = {};
        double result = -1;

        Class<?> sunBeanClass;
        try {
            sunBeanClass = Class.forName("com.sun.management.OperatingSystemMXBean");
            Object sunBean = ManagementFactory.getOperatingSystemMXBean();

            Method sunMethod = sunBeanClass.getDeclaredMethod(cpuLoadMethod, noparams);
            Double sunResult = (Double) sunMethod.invoke(sunBean, (Object[]) null);
            if (sunResult != null) {
                result = sunResult.doubleValue();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

}