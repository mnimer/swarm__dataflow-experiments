package com.mikenimer.swarm.windows;

import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;

import java.util.List;

public interface JobOptions extends GcpOptions {

    String getBqTable();
    void setBqTable(String value);

    String[] getBqColumnList();
    void setBqColumnList(String[] value);

    String getDestTable();
    void setDestTable(String value);

    String[] getDestColumnList();
    void setDestColumnList(String[] value);

    String getDriver() ;
    void setDriver(String value);

    String getUrl() ;
    void setUrl(String value);

    String getUsername() ;
    void setUsername(String value);

    String getPassword() ;
    void setPassword(String value);
}