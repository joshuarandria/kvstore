package org.example.kvstore;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class StoreTest {
    public static final boolean DEBUG = false;

    @Test
    public void baseOperations() {
        StoreManager manager = new StoreManager();
        Store<Integer, Integer> store = manager.newStore();

        assert store.get(1) == null;
        if (DEBUG) {
            System.out.println();
            System.out.println();
            System.out.println(" ######### BEGIN put(42,1)  #########");
        }
        store.put(42, 1);

        if (DEBUG)
            System.out.println(store.toString());

        assert store.get(42).equals(1);

        assert store.put(42, 2).equals(1);

        System.out.println("baseOperations ok");
    }

    @Test
    public void multipleStores() {
        int NCALLS = 1000;
        Random rand = new Random(System.nanoTime());

        StoreManager manager = new StoreManager();
        if (DEBUG)
            System.out.println("store1");
        Store<Integer, Integer> store1 = manager.newStore();
        if (DEBUG)
            System.out.println("store2");
        Store<Integer, Integer> store2 = manager.newStore();
        if (DEBUG)
            System.out.println("store3");
        Store<Integer, Integer> store3 = manager.newStore();

        List<Store<Integer, Integer>> stores = new ArrayList<>();

        stores.add(store1);
        stores.add(store2);
        stores.add(store3);

        for (int i = 0; i < NCALLS; i++) {
            int k = rand.nextInt();
            int v = rand.nextInt();
            if (DEBUG)
                System.out.println("###### command put    k:" + k + "   ,   v=" + v + "   ######");
            store1.put(k, v);
            if (DEBUG) {
                System.out.println("############## command put  done  #############");
                for (Store<Integer, Integer> store : stores) {
                    System.out.println(store.toString());
                }
                System.out.println("test store2 " + store2.get(k).equals(v));
                System.out.println("test store3 " + store3.get(k).equals(v));
            }
            if (DEBUG)
                System.out.println("############## check with a get #############");
            assert rand.nextBoolean() ? store2.get(k).equals(v) : store3.get(k).equals(v);
        }

        System.out.println();
        System.out.println("multipleStores ok");
    }

    @Test
    public void dataMigration() {
        int NCALLS = 99;
        Random rand = new Random(System.nanoTime());

        StoreManager manager = new StoreManager();
        if (DEBUG)
            System.out.println("store1");
        Store<Integer, Integer> store1 = manager.newStore();

        List<Store<Integer, Integer>> stores = new ArrayList<>();
        List<Integer> keys = new ArrayList<>();

        stores.add(store1);

        for (int i = 0; i < NCALLS; i++) {
            int k = rand.nextInt();
            keys.add(k);
            int v = i;
            if (DEBUG)
                System.out.println("\n ############## PUT (" + k + "," + v + ")       #############");
            store1.put(k, v);
            if (DEBUG)
                System.out.println(" ############## END PUT (" + k + "," + v + ")       #############\n");
        }
        if (DEBUG)
            System.out.println("\n\nStore1: " + store1.toString());

        if (DEBUG)
            System.out.println("_________________    store2    _________________");
        Store<Integer, Integer> store2 = manager.newStore();
        stores.add(store2);

        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        if (DEBUG) {
            System.out.println("----------- after store2 creation  ------------");
            System.out.println();
            System.out.println("store1: " + store1.toString());
            System.out.println("store2: " + store2.toString());

            System.out.println();
        }

        for (int i = NCALLS + 1; i < NCALLS + NCALLS; i++) {
            int k = rand.nextInt();
            keys.add(k);
            int v = i;
            if (DEBUG)
                System.out.println("\n ############## PUT (" + k + "," + v + ") #############");
            store1.put(k, v);
            if (DEBUG)
                System.out.println(" ############## END PUT (" + k + "," + v + ") #############\n");
        }
        if (DEBUG)
            System.out.println("_________________ store3 _________________");
        Store<Integer, Integer> store3 = manager.newStore();
        stores.add(store3);

        if (DEBUG) {
            System.out.println("----------- after store3 creation ------------");
            System.out.println();
            System.out.println("store1: " + store1.toString());
            System.out.println();
            System.out.println("store2: " + store2.toString());
            System.out.println();
            System.out.println("store3: " + store3.toString());
        }
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        for (int k : keys) {
            assert store1.get(k).equals(store2.get(k));
            assert store2.get(k).equals(store3.get(k));
        }

        System.out.println();
        System.out.println("dataMigration ok");
    }
}