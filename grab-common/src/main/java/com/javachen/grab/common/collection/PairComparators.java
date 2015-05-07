package com.javachen.grab.common.collection;

import java.util.Comparator;

public final class PairComparators {

    private PairComparators() {
    }

    public static <K extends Comparable<K>> Comparator<Pair<K, ?>> byFirst() {
        return new Comparator<Pair<K, ?>>() {
            @Override
            public int compare(Pair<K, ?> p1, Pair<K, ?> p2) {
                return p1.getFirst().compareTo(p2.getFirst());
            }
        };
    }

    public static <V extends Comparable<V>> Comparator<Pair<?, V>> bySecond() {
        return new Comparator<Pair<?, V>>() {
            @Override
            public int compare(Pair<?, V> p1, Pair<?, V> p2) {
                return p1.getSecond().compareTo(p2.getSecond());
            }
        };
    }

}
