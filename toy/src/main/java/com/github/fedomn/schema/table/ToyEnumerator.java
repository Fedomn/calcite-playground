package com.github.fedomn.schema.table;

import com.github.fedomn.util.Util;
import org.apache.calcite.linq4j.Enumerator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ToyEnumerator<E> implements Enumerator<E> {

    private int currentRowIdx;
    private List data;

    public ToyEnumerator(String filename) throws IOException {
        this.currentRowIdx = -1;
        this.data = new ArrayList<>();

        String content = Util.fileContent(filename);
        for (String s : content.split("\n")) {
            String[] items = s.split(" ");
            data.add(items);
        }
    }

    @Override
    public E current() {
        return (E) this.data.get(currentRowIdx);
    }

    @Override
    public boolean moveNext() {
        currentRowIdx++;
        return currentRowIdx < this.data.size();
    }

    @Override
    public void reset() {
        currentRowIdx = -1;
    }

    @Override
    public void close() {
        currentRowIdx = -1;
    }
}
