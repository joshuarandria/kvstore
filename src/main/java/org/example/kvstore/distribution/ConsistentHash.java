package org.example.kvstore.distribution;

import org.jgroups.Address;
import org.jgroups.View;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;

public class ConsistentHash implements Strategy {

  private TreeSet<Integer> ring;
  private Map<Integer, Address> addresses;
  public static final boolean DEBUG = false;

  public ConsistentHash(View view) {
    if (DEBUG)
      System.out.println("_____ create ConsistentHash  _____");
    ring = new TreeSet<>();
    addresses = new HashMap<>();
    for (Address addr : view.getMembers()) {
      ring.add(addr.hashCode());
      addresses.put(addr.hashCode(), addr);
    }
    if (DEBUG) {
      System.out.println("addresses " + addresses);
      System.out.println("ring " + ring);
      System.out.println("_____ ConsistentHash created  _____");
      System.out.println();
    }
  }

  @Override
  public Address lookup(Object key) {
    int nbucket = 0;
    if (ring.higher(key.hashCode()) != null) {
      nbucket = ring.higher(key.hashCode());
    } else {
      if (DEBUG)
        System.out.println("key in last part before address 0 so for first bucket");
      nbucket = ring.first();
    }
    if (DEBUG) {
      System.out.println("Destination= " + addresses.get(nbucket));
    }
    return addresses.get(nbucket);
  }
}