package dev.jarcadia.redao.exception;

public class ProxyException extends RuntimeException {

    public ProxyException(String message)
    {
        super(message);
    }

    public ProxyException(Throwable cause)
    {
        super(cause);
    }

    public ProxyException(String message, Throwable cause)
    {
        super(message, cause);
    }
}
