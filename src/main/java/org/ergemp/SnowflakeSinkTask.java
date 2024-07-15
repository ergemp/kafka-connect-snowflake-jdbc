package org.ergemp;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.ergemp.sink.SnowflakeJdbcSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;

public class SnowflakeSinkTask extends SinkTask {

    private static final Logger log = LoggerFactory.getLogger(SnowflakeSinkTask.class);

    private SnowflakeJdbcSink jdbcSink = new SnowflakeJdbcSink();

    public SnowflakeSinkTask(){
    }

    @Override
    public String version() {
        return new SnowflakeSinkConnector().version();
    }

    @Override
    public void start(Map<String, String> props) {
        AbstractConfig config = new AbstractConfig(SnowflakeSinkConnector.CONFIG_DEF, props);

        jdbcSink.setJdbcUrl("jdbc:snowflake://" + config.getString("snowflakeUrl"));
        //jdbcSink.setJdbcUrl("jdbc:snowflake://xxx.snowflakecomputing.com/");

        jdbcSink.addProperty("user", config.getString("userName"));
        jdbcSink.addProperty("password", config.getString("password"));
        jdbcSink.addProperty("account", "XI02455");
        jdbcSink.addProperty("warehouse", "sf_tuts_wh");
        jdbcSink.addProperty("db", "sf_tuts");
        jdbcSink.addProperty("schema", "public");

        try {
            jdbcSink.setConnection(DriverManager.getConnection(jdbcSink.getJdbcUrl(), jdbcSink.getProperties()));
            jdbcSink.getStatement();
        }
        catch(Exception ex){
        }
        finally {}
    }

    @Override
    public void put(Collection<SinkRecord> collection) {
        for (SinkRecord record : collection) {
            jdbcSink.executeStatement("insert into jdbc_sink_table values ('" + record.value() + "')");
            //log.trace("Writing line: {}", record.value());
        }
    }

    @Override
    public void stop() {
    }

}
