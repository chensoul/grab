package com.javachen.grab.common.lang;

public final class LangUtils {

    private LangUtils() {
    }

    /**
     * Like {@link com.google.common.primitives.Doubles#hashCode(double)} but avoids creating
     * a whole new object!
     *
     * @param d double to hash
     * @return the same value produced by {@link Double#hashCode()}
     */
    public static int hashDouble(double d) {
        long bits = Double.doubleToLongBits(d);
        return (int) (bits ^ (bits >>> 32));
    }

}
