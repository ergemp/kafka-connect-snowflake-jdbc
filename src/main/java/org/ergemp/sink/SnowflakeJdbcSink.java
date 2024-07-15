package org.ergemp.sink;

import org.apache.kafka.connect.errors.ConnectException;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Properties;

public class SnowflakeJdbcSink {
    private Properties properties ;
    private String jdbcUrl;
    private Connection connection;
    private Statement statement;

    public SnowflakeJdbcSink(){
        this.properties = new Properties();
    }
    public void addProperty(Object key, Object value){
        this.properties.put(key, value);
    }
    public Properties getProperties() {
        return properties;
    }
    public void setJdbcUrl(String gUrl){
        jdbcUrl = gUrl;
    }
    public String getJdbcUrl(){
        return  jdbcUrl;
    }
    public void setConnection(Connection gConn){
        connection = gConn;
    }
    public void getStatement(){
        try {
            statement = connection.createStatement();
        }
        catch(Exception ex){
            throw new ConnectException("Cannot connect to snowflake", ex);
        }
        finally {}
    }
    public void executeStatement(String gStr){
        try {
            statement.executeUpdate(gStr);
        }
        catch(Exception ex){
            throw new ConnectException("Cannot execute statement", ex);
        }
        finally {}
    }
}
