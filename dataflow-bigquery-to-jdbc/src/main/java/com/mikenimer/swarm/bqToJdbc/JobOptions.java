package com.mikenimer.swarm.bqToJdbc;

import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;

import java.util.List;

public interface JobOptions extends GcpOptions {

    /**
     * Comma separated list of tables to copy over
     */
    String[] getBqTable();
    void setBqTable(String[] value);


    /**
     * Name of jdbc database to save data. Table names will be the same as bq.
     * @return
     */
    String getDestDatabase();
    void setDestDatabase(String value);


    /**
     * JDBC Connection string props
     * @return
     */
    String getDriver() ;
    void setDriver(String value);

    String getUrl() ;
    void setUrl(String value);


    // TODO: replace username/password with a call to SECRET MANAGER
    String getUsername() ;
    void setUsername(String value);

    String getPassword() ;
    void setPassword(String value);
}