package com.yr.conf;

import com.yr.constant.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Properties;

/**
 * Description 配置管理
 * @return
 * @Author dengbp
 * @Date 11:38 2020-03-12
 **/
final public class ConfigurationManager {
	private static Logger logger = LoggerFactory.getLogger(ConfigurationManager.class);

	private static Properties prop = new Properties();

	static {
		try {
			InputStream in = ConfigurationManager.class
					.getClassLoader().getResourceAsStream("big.properties");
			prop.load(in);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 获取指定key对应的value
	 * @param key
	 * @return value
	 */
	public static String getProperty(String key) {
		return prop.getProperty(key);
	}

	/**
	 * 获取整数类型的配置项
	 * @param key
	 * @return value
	 */
	public static Integer getInteger(String key) {
		String value = getProperty(key);
		try {
			return Integer.valueOf(value);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return 0;
	}

	/**
	 * 获取布尔类型的配置项
	 * @param key
	 * @return value
	 */
	public static Boolean getBoolean(String key) {
		String value = getProperty(key);
		try {
			return Boolean.valueOf(value);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}

	/**
	 * 获取Long类型的配置项
	 * @param key
	 * @return
	 */
	public static Long getLong(String key) {
		String value = getProperty(key);
		try {
			return Long.valueOf(value);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return 0L;
	}

	public static Configuration getHBaseConfiguration() {
		Configuration conf = null;
		try {
			conf = HBaseConfiguration.create();
			conf.set("hbase.zookeeper.quorum", ConfigurationManager
					.getProperty(Constants.ZK_METADATA_BROKER_LIST));
			conf.set("hbase.defaults.for.version.skip", "true");

		} catch (Exception e) {
			logger.error("获取HBaseConfiguration出错，请检查是否有配置文件和ZK是否正常。ZK链接： ->"
					+ ConfigurationManager
					.getProperty(Constants.ZK_METADATA_BROKER_LIST));
		}
		return conf;
	}

}
