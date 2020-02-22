package com.jarcadia.rcommando.exception;

public class RedisCommandoException extends RuntimeException {

    public RedisCommandoException(String message)
    {
        super(message);
    }

    public RedisCommandoException(Throwable cause)
    {
        super(cause);
    }

    public RedisCommandoException(String message, Throwable cause)
    {
        super(message, cause);
    }
}
