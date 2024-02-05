package com.mikenimer.swarm.launch;

import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;

public interface JobOptions extends GcpOptions {


    String getSubscription();
    void setSubscription(String value);

    String getTemplate();
    void setTemplate(String value);
}