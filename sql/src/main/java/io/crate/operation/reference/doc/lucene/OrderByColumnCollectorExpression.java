package io.crate.operation.reference.doc.lucene;

import io.crate.types.DataType;

public class OrderByColumnCollectorExpression extends LuceneCollectorExpression<Object> {

    private Object value;

    private final DataType valueType;

    public OrderByColumnCollectorExpression(DataType valueType) {
        this.valueType = valueType;
    }

    public void value(Object value) {
        this.value = value;
    }

    @Override
    public Object value() {
        return value;
    }

    public DataType valueType() {
        return valueType;
    }
}
