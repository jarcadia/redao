package com.jarcadia.rcommando;

public class RcException extends RuntimeException {

    public RcException(String message)
    {
        super(message);
    }

    public RcException(Throwable cause)
    {
        super(cause);
    }

    public RcException(String message, Throwable cause)
    {
        super(message, cause);
    }
}
