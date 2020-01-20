package wcc.common;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.logging.Logger;


public final class Configuration {
	private final List<PID> processes;
	private final Properties configuration = new Properties();
	/**
	 * Loads the configuration from the default file
	 * <code>fpsi.properties</code>
	 * 
	 * @throws IOException
	 */
	
	public Configuration() throws IOException {
		this("fpsi.properties");
	}
	/**
	 * Loads the configuration from the given file.
	 * 
	 * @param confFile
	 * @throws IOException
	 */
	
	public Configuration(String confFile) throws IOException {
		// Load property from file there is one
		FileInputStream fis = new FileInputStream(confFile);
		configuration.load(fis);
		fis.close();
		logger.info("Configuration loaded from file: " + confFile);
		this.processes = Collections.unmodifiableList(loadProcessList());
	}

	/**
	 * Creates a configuration with the process list, and an empty set of
	 * optional properties.
	 * 
	 * @param processes
	 */
	public Configuration(List<PID> processes) {
		this.processes = processes;
	}

	public int getN() {
		return processes.size();
	}

	public List<PID> getProcesses() {
		return processes;
	}
	
	public PID getProcess(int id) {
		return processes.get(id);
	}

	public boolean containsKey(String key) {
		return configuration.containsKey(key);
	}

	/**
	 * Returns a given property, converting the value to an integer.
	 * 
	 * @param key - the key identifying the property
	 * @param defValue - the default value to use in case the key is not found.
	 * @return the value of key property or defValue if key not found
	 */
	public int getIntProperty(String key, int defValue) {
		String str = configuration.getProperty(key);
		if (str == null) {
			logger.fine("Property not found: " + key + ". Using default value: " +
					defValue);
			return defValue;
		}
		return Integer.parseInt(str);
	}

	/**
	 * Returns a given property, converting the value to a boolean.
	 * 
	 * @param key - the key identifying the property
	 * @param defValue - the default value to use in case the key is not found.
	 * @return the value of key property or defValue if key not found
	 */
	public boolean getBooleanProperty(String key, boolean defValue) {
		String str = configuration.getProperty(key);
		if (str == null) {
			logger.fine("Property not found: " + key + ". Using default value: " + defValue);
			return defValue;
		}
		return Boolean.parseBoolean(str);
	}

	/**
	 * 
	 * @param key - the key identifying the property
	 * @param defValue - the default value to use in case the key is not found.
	 * 
	 * @return the value of key property or defValue if key not found
	 */
	public String getProperty(String key, String defValue) {
		String str = configuration.getProperty(key);
		if (str == null) {
			logger.fine("Property not found: " + key + ". Using default value: " + defValue);
			return defValue;
		}
		return str;
	}

	private List<PID> loadProcessList() {
		List<PID> processes = new ArrayList<PID>();
		int i = 0;
		boolean local_execution = Boolean.parseBoolean(configuration.getProperty("run_locally"));
		if (!local_execution) {
			System.out.println("Remote Execution!");
			try {
				String port_2pc = configuration.getProperty("port_2pc");
				String port_client = configuration.getProperty("port_client");
				String port_batch = configuration.getProperty("port_batch");

				BufferedReader br = new BufferedReader(new FileReader(configuration.getProperty("cluster_file")));
				String line = br.readLine();
				while (line != null) {

					PID pid = new PID(i, line, Integer.parseInt(port_2pc), Integer.parseInt(port_client));
					processes.add(pid);
					logger.info(pid.toString());
					i++;
					line = br.readLine();
				}
				br.close();

			} catch (IOException e) {
				System.out.println("No cluster.txt file");
				e.printStackTrace();
				System.exit(0);
			}
		}else {
			System.out.println("Local Execution!");
			while (true) {
				//use another file (just names of the servers)
				String line = configuration.getProperty("process." + i);
				if (line == null) {
					break;
				}
				StringTokenizer st = new StringTokenizer(line, ":");
				PID pid = new PID(i, st.nextToken(), Integer.parseInt(st.nextToken()),
						Integer.parseInt(st.nextToken()));
				processes.add(pid);
				logger.info(pid.toString());
				i++;
			}
		}
		System.out.print("List of Processes: [");
		for(PID pid : processes) {
			System.out.print(pid.getHostname()+";");
		}
		System.out.println("]");
		return processes;
	}

	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("Processes:\n");
		for (PID p : processes) {
			sb.append("  ").append(p).append("\n");
		}
		sb.append("Properties:\n");
		for (Object key : configuration.keySet()) {
			sb.append("  ").append(key).append("=").append(configuration.get(key)).append("\n");
		}

		return sb.toString();
	}

	public double getDoubleProperty(String key, double defultValue) {
		String str = configuration.getProperty(key);
		if (str == null) {
			logger.fine("Property not found: " + key + ". Using default value: " + defultValue);
			return defultValue;
		}
		return Double.parseDouble(str);
	}

	public long getLongProperty(String key, long defultValue) {
		String str = configuration.getProperty(key);
		if (str == null) {
			logger.fine("Property not found: " + key + ". Using default value: " + defultValue);
			return defultValue;
		}
		return Long.parseLong(str);
	}

	private final static Logger logger = Logger.getLogger(Configuration.class.getCanonicalName());
}
