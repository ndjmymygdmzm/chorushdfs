package com.emc.greenplum.hadoop.plugins;


public class HdfsPair {

    private String key;
    private String value;

    public HdfsPair() {
        this.key = "";
        this.value = "";
    }

    public HdfsPair(String _key, String _value) {
        this.key = _key;
        this.value = _value;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
