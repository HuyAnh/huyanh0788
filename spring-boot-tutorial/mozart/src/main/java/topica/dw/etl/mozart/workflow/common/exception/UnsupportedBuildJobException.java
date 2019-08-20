package topica.dw.etl.mozart.workflow.common.exception;

public class UnsupportedBuildJobException extends Exception {
    private static final long serialVersionUID = -4895886913862751259L;

    public UnsupportedBuildJobException(String message) {
        super(message);
    }

    public UnsupportedBuildJobException(String message, Throwable e) {
        super(message, e);
    }

}
