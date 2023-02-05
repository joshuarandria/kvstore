package org.example.kvstore.cmd;

public class Migr<K, V> extends Command<K, V> {

    public Migr() {
        super(null, null);
    }

    @Override
    public String toString() {
        return "Migr";
    }

}
