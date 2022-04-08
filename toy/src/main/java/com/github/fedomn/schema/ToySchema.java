package com.github.fedomn.schema;

import com.github.fedomn.schema.table.ToyTable;
import com.google.common.collect.Maps;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import java.util.List;
import java.util.Map;

public class ToySchema extends AbstractSchema {
    public String name;

    public List<ToyTable> tables;

    public ToySchema(String name, List<ToyTable> tables) {
        this.name = name;
        this.tables = tables;
    }

    @Override
    protected Map<String, Table> getTableMap() {
        Map<String, Table> toyTableMap = Maps.newHashMap();
        for (ToyTable table : tables) {
            toyTableMap.put(table.name, table);
        }
        return toyTableMap;
    }
}
