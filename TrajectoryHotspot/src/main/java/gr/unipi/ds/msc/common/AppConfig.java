package gr.unipi.ds.msc.common;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class AppConfig {
    private static Config conf = null;

    public static Config initiate(String filename) {
        if (filename == null) {
            conf = ConfigFactory.load();
        }
        else {
            conf = ConfigFactory.load(filename);
        }
        return conf;
    }

    public static Config getConfig() {
        return conf;
    }
}
