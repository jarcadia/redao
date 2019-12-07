package com.jarcadia.rcommando;

public class RedisException extends RuntimeException {

    public RedisException(String message)
    {
        super(message);
    }

    public RedisException(Throwable cause)
    {
        super(cause);
    }

    public RedisException(String message, Throwable cause)
    {
        super(message, cause);
    }
}
