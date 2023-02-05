package org.example.kvstore.distribution;

import org.jgroups.Address;
// import org.jgroups.View;

public interface Strategy {
    Address lookup(Object key);
    // void update(View view);
}
