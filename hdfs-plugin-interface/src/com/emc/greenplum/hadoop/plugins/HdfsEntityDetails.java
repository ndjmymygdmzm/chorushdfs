package com.emc.greenplum.hadoop.plugins;

import java.text.SimpleDateFormat;
import java.util.Date;

public class HdfsEntityDetails {
    private String owner;
    private String group;
    private String accessedAt;
    private String modifiedAt;
    private long blockSize;
    private long size;
    private short replication;
    private String permissions;

    private static SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    
    public HdfsEntityDetails(
            String owner,
            String group,
            long accessedAt,
            long modifiedAt,
            long blockSize,
            long size,
            short replication,
            String permissions
    ) {
        this.owner = owner;
        this.group = group;
        this.accessedAt = toDateString(accessedAt);
        this.modifiedAt = toDateString(modifiedAt);
        this.blockSize = blockSize;
        this.size = size;
        this.replication = replication;
        this.permissions = permissions;
    }

    public HdfsEntityDetails() {}

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public String getAccessedAt() {
        return accessedAt;
    }

    public void setAccessedAt(long accessedAt) {
        this.accessedAt = toDateString(accessedAt);
    }

    public String getModifiedAt() {
        return modifiedAt;
    }

    public void setModifiedAt(long modifiedAt) {
        this.modifiedAt = toDateString(modifiedAt);
    }

    public long getBlockSize() {
        return blockSize;
    }

    public void setBlockSize(long blockSize) {
        this.blockSize = blockSize;
    }

    public long getSize() {
        return size;
    }

    public void setSize(long size) {
        this.size = size;
    }

    public short getReplication() {
        return replication;
    }

    public void setReplication(short replication) {
        this.replication = replication;
    }

    public String getPermissions() {
        return permissions;
    }

    public void setPermissions(String permissions) {
        this.permissions = permissions;
    }

    private static String toDateString(long val) {
        return format.format(new Date(val));
    }
}
