package dev.jarcadia.redao.exception;

public class RcDeserializationException extends Exception {

    public RcDeserializationException(String message)
    {
        super(message);
    }

    public RcDeserializationException(Throwable cause)
    {
        super(cause);
    }

    public RcDeserializationException(String message, Throwable cause)
    {
        super(message, cause);
    }
}
