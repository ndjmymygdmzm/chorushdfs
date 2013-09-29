package com.emc.greenplum.hadoop.plugins;

import java.text.SimpleDateFormat;
import java.util.Date;

public class HdfsEntity {
    private String path;
    private Date modifiedAt;
    private boolean isDirectory;
    private long size;
    private int contentCount = 0;

    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getModifiedAt() {
        return format.format(modifiedAt);
    }

    public void setModifiedAt(Date modifiedAt) {
        this.modifiedAt = modifiedAt;
    }

    public boolean isDirectory() {
        return isDirectory;
    }

    public void setDirectory(boolean directory) {
        isDirectory = directory;
    }

    public long getSize() {
        return size;
    }

    public void setSize(long size) {
        this.size = size;
    }

    public void setContentCount(int length) {
        this.contentCount = length;
    }

    public int getContentCount() {
        return this.contentCount;
    }
}
