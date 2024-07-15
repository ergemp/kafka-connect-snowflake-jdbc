package org.ergemp;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SnowflakeSinkConnector extends SinkConnector {

    private Map<String, String> props;
    static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define("snowflakeUrl", ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, "Snowflake Connection URL")
            .define("userName", ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, "Snowflake Username")
            .define("password", ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, "Snowflake Password")
            ;

    @Override
    public void start(Map<String, String> map) {
        this.props = map;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return SnowflakeSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        ArrayList<Map<String, String>> configs = new ArrayList<>();
        for (int i = 0; i < maxTasks; i++) {
            configs.add(props);
        }
        return configs;
    }

    @Override
    public void stop() {

    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public String version() {
        return "20230922-v1";
        //return AppInfoParser.getVersion();
        //return null;
    }

    public String author() {
        return "org.ergemp";
        //return AppInfoParser.getVersion();
        //return null;
    }
}
