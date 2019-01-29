package com.amazonaws.services.neptune.auth;

public class HandshakeRequestConfig {

    public static HandshakeRequestConfig create(String nlbHostHeader, String albHostHeader){
        if (nlbHostHeader == null && albHostHeader == null){
            return new HandshakeRequestConfig(null, false);
        } else if (nlbHostHeader != null){
            return new HandshakeRequestConfig(nlbHostHeader, false);
        } else {
            return new HandshakeRequestConfig(albHostHeader, true);
        }
    }

    public static HandshakeRequestConfig parse(String s){
        String[] values = s.split(",");
        boolean removeHostHeaderAfterSigning = Boolean.parseBoolean(values[0]);
        String hostHeader = values[1];

        return new HandshakeRequestConfig(hostHeader, removeHostHeaderAfterSigning);
    }

    private final String hostHeader;
    private final boolean removeHostHeaderAfterSigning;

    private HandshakeRequestConfig(String hostHeader, boolean removeHostHeaderAfterSigning) {
        this.hostHeader = hostHeader;
        this.removeHostHeaderAfterSigning = removeHostHeaderAfterSigning;
    }

    public String hostHeader() {
        return hostHeader;
    }

    public boolean removeHostHeaderAfterSigning() {
        return removeHostHeaderAfterSigning;
    }

    public String value(){
        return String.format("%s,%s", removeHostHeaderAfterSigning, hostHeader);
    }

    public boolean isEmpty(){
        return hostHeader == null;
    }

    @Override
    public String toString() {
        return value();
    }
}
