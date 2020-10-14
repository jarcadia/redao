package dev.jarcadia.redao;

import io.lettuce.core.KeyValue;

class ExternalUpdatePopperRepository {

    private final RedaoCommando rcommando;

    public ExternalUpdatePopperRepository(RedaoCommando rcommando) {
        this.rcommando = rcommando;
    }

    protected String popUpdate() {
        KeyValue<String, String> popped = rcommando.core().blpop(1, "updates");
        if (popped == null) {
            return null;
        } else {
            return popped.getValue();
        }
    }

    protected void close() {
        rcommando.close();
    }
}


