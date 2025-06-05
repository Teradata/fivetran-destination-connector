package com.teradata.fivetran.destination.writers;

public class ColumnMetadata {
    private final int length;
    private final int charType; // 1=LATIN, 2=UNICODE

    public ColumnMetadata(int length, int charType) {
        this.length = length;
        this.charType = charType;
    }

    public int getLength() {
        return length;
    }

    public boolean isUnicode() {
        return charType == 2;
    }

    public int getMaxAllowedLength() {
        return isUnicode() ? 32000 : 64000;
    }

    @Override
    public String toString() {
        return String.format("length=%d, charset=%s", length, isUnicode() ? "UNICODE" : "LATIN");
    }
}
