package com.github.fedomn.schema.table;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ToyTable extends AbstractTable implements ScannableTable {
    public String name;
    public List<ToyColumn> columns;
    public String filename;

    public ToyTable() {
    }

    public ToyTable(String name, List<ToyColumn> columns) {
        this.name = name;
        this.columns = columns;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        List<String> names = new ArrayList<>();
        List<RelDataType> types = new ArrayList<>();
        for (var column : columns) {
            names.add(column.name);
            RelDataType sqlType = typeFactory.createSqlType(SqlTypeName.get(column.type.toUpperCase()));
            types.add(sqlType);
        }
        return typeFactory.createStructType(Pair.zip(names, types));
    }

    public void setFilename(String filename) {
        this.filename = filename;
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root) {
        return new AbstractEnumerable<>() {
            @Override
            public Enumerator<Object[]> enumerator() {
                try {
                    return new ToyEnumerator<>(filename);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }

    @Override
    public String toString() {
        return "ToyScannableTable";
    }

}
