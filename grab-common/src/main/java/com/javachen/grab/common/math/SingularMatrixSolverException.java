package com.javachen.grab.common.math;

public final class SingularMatrixSolverException extends RuntimeException {

    private final int apparentRank;

    public SingularMatrixSolverException(int apparentRank, String message) {
        super(message);
        this.apparentRank = apparentRank;
    }

    public int getApparentRank() {
        return apparentRank;
    }

}
