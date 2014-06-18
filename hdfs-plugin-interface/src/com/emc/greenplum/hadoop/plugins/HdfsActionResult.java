package com.emc.greenplum.hadoop.plugins;

public class HdfsActionResult {
    private boolean success;
    private Exception exception;
    private String message;

    public HdfsActionResult(boolean success) {
        this.success = success;
    }

    public HdfsActionResult(boolean success, String message, Exception exception) {
        this.success = success;
        this.message = message;
        this.exception = exception;
    }

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public Exception getException() {
        return exception;
    }

    public void setException(Exception exception) {
        this.exception = exception;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}
