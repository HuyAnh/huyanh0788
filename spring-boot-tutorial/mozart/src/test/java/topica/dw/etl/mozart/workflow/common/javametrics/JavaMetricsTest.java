package topica.dw.etl.mozart.workflow.common.javametrics;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.lang.management.MemoryUsage;
import java.util.Iterator;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.MBeanServer;
import javax.management.ObjectName;

public class JavaMetricsTest {

	public static void main(String args[]) throws Exception {
		Thread t = new Thread(new Runnable() {
			int count = 0;

			@Override
			public void run() {
				while (true) {
					count++;
					try {
						Thread.sleep(100);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
		});
		t.start();
		System.out.println(getProcessCpuLoad());
		System.out.println("All thread:" + ThreadUtils.getInstance().getAllThreads().size());
		System.out.println(MemoryPoolDataProvider.getHeapMemory());
	}

	public static double getProcessCpuLoad() throws Exception {
		MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
		ObjectName name = ObjectName.getInstance("java.lang:type=OperatingSystem");
		AttributeList list = mbs.getAttributes(name, new String[] { "SystemCpuLoad" });

		if (list.isEmpty())
			return Double.NaN;

		Attribute att = (Attribute) list.get(0);
		Double value = (Double) att.getValue();

		// usually takes a couple of seconds before we get real values
		if (value == -1.0)
			return Double.NaN;
		// returns a percentage value with 1 decimal point precision
		return value;
	}

	public static void detectMemoryUsage() {
		StringBuilder sb = new StringBuilder();
		Iterator<MemoryPoolMXBean> iter = ManagementFactory.getMemoryPoolMXBeans().iterator();
		while (iter.hasNext()) {
			MemoryPoolMXBean item = iter.next();
			String name = item.getName();
			MemoryType type = item.getType();
			MemoryUsage usage = item.getUsage();
			MemoryUsage peak = item.getPeakUsage();
			MemoryUsage collections = item.getCollectionUsage();
			sb.append(String.format("Memory pool name: " + name + ", type: " + type + ", usage: " + usage + ", peak: "
					+ peak + ", collections: " + collections + "\n"));
		}
		System.out.println(sb.toString());
	}

}
