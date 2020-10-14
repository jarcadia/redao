package dev.jarcadia.redao;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import io.lettuce.core.KeyValue;

public class DaoValues implements Iterable<DaoValue> {
    
    private final ValueFormatter formatter;
    private final Iterator<DaoValue> iter;
    
    protected DaoValues(ValueFormatter formatter, List<KeyValue<String, String>> valueList) {
        this.formatter = formatter;
        final Iterator<KeyValue<String, String>> sourceIter = valueList.iterator();
        this.iter = new Iterator<>() {
            @Override
            public boolean hasNext() {
                return sourceIter.hasNext();
            }

            @Override
            public DaoValue next() {
                KeyValue<String, String> val = sourceIter.next();
                return new DaoValue(formatter, val.getKey(), val.getValueOrElse(null));
            }
        };
    }

    protected DaoValues(ValueFormatter formatter, Map<String, String> valueMap) {
        this.formatter = formatter;
        final Iterator<Map.Entry<String, String>> sourceIter = valueMap.entrySet().iterator();
        this.iter = new Iterator<>() {
            @Override
            public boolean hasNext() {
                return sourceIter.hasNext();
            }

            @Override
            public DaoValue next() {
                Map.Entry<String, String> val = sourceIter.next();
                return new DaoValue(formatter, val.getKey(), val.getValue());
            }
        };
    }

    @Override
    public Iterator<DaoValue> iterator() {
        return iter;
    }

    public Pair asPair() {
        return new Pair(iter.next(), iter.next());
    }

    public Triplet asTriplet() {
        return new Triplet(iter.next(), iter.next(), iter.next());
    }

    public static class Pair {
        private final DaoValue value0;
        private final DaoValue value1;

        private Pair(DaoValue value0, DaoValue value1) {
            this.value0 = value0;
            this.value1 = value1;
        }

        public DaoValue getValue0() {
            return value0;
        }

        public DaoValue getValue1() {
            return value1;
        }
    }

    public static class Triplet {

        private final DaoValue value0;
        private final DaoValue value1;
        private final DaoValue value2;

        private Triplet(DaoValue value0, DaoValue value1, DaoValue value2) {
            this.value0 = value0;
            this.value1 = value1;
            this.value2 = value2;
        }

        public DaoValue getValue0() {
            return value0;
        }

        public DaoValue getValue1() {
            return value1;
        }

        public DaoValue getValue2() {
            return value2;
        }
    }
}
