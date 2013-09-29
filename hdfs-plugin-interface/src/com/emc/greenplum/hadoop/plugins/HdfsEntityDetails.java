package com.emc.greenplum.hadoop.plugins;

public class HdfsEntityDetails {
    private String owner;
    private String group;
    private long accessedAt;
    private long modifiedAt;
    private long blockSize;
    private long size;
    private short replication;
    private String permission;
    
    public HdfsEntityDetails(
            String owner,
            String group,
            long accessedAt,
            long modifiedAt,
            long blockSize,
            long size,
            short replication,
            String permission
    ) {
        this.owner = owner;
        this.group = group;
        this.accessedAt = accessedAt;
        this.modifiedAt = modifiedAt;
        this.blockSize = blockSize;
        this.size = size;
        this.replication = replication;
        this.permission = permission;
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

    public long getAccessedAt() {
        return accessedAt;
    }

    public void setAccessedAt(long accessedAt) {
        this.accessedAt = accessedAt;
    }

    public long getModifiedAt() {
        return modifiedAt;
    }

    public void setModifiedAt(long modifiedAt) {
        this.modifiedAt = modifiedAt;
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

    public String getPermission() {
        return permission;
    }

    public void setPermission(String permission) {
        this.permission = permission;
    }
}
