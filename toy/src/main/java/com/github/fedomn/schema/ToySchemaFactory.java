package com.github.fedomn.schema;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.github.fedomn.schema.table.ToyTable;
import com.github.fedomn.util.Util;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ToySchemaFactory implements SchemaFactory {
    public static final ToySchemaFactory INSTANCE = new ToySchemaFactory();
    private static final ObjectMapper MAPPER = new JsonMapper();

    @Override
    public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
        try {
            List<ToyTable> resultTables = new ArrayList<>();

            var tables = (ArrayList) operand.get("tables");
            for (Object table : tables) {
                var ddl = (String) ((HashMap<?, ?>) table).get("ddl");
                var data = (String) ((HashMap<?, ?>) table).get("data");
                String content = Util.fileContent(ddl);
                ToyTable toyTable = MAPPER.readValue(content, ToyTable.class);
                toyTable.setFilename(data);
                resultTables.add(toyTable);
            }
            return new ToySchema(name, resultTables);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
