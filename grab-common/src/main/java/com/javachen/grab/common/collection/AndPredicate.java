package com.javachen.grab.common.collection;

import net.openhft.koloboke.function.Predicate;

public final class AndPredicate<T> implements Predicate<T> {

    // Consider supporting arbitrary # later
    private final Predicate<T> a;
    private final Predicate<T> b;

    public AndPredicate(Predicate<T> a, Predicate<T> b) {
        this.a = a;
        this.b = b;
    }

    @Override
    public boolean test(T value) {
        return a.test(value) && b.test(value);
    }
}
