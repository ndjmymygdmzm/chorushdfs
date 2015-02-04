package com.emc.greenplum.hadoop.plugins;

import java.util.Date;
import org.apache.hadoop.security.UserGroupInformation;

public class HdfsExpirableUGI {

    private UserGroupInformation userGroupInformation;
    private Date timeCreated;

    public HdfsExpirableUGI() {
        this.userGroupInformation = null;
        this.timeCreated = new Date(System.currentTimeMillis());
    }

    public HdfsExpirableUGI(UserGroupInformation ugi) {
        this.userGroupInformation = ugi;
        this.timeCreated = new Date(System.currentTimeMillis());
    }

    public UserGroupInformation getUserGroupInformation() {
        return userGroupInformation;
    }

    public void setUserGroupInformation(UserGroupInformation userGroupInformation) {
        this.userGroupInformation = userGroupInformation;
    }

    public Date getTimeCreated() {
        return timeCreated;
    }
}
