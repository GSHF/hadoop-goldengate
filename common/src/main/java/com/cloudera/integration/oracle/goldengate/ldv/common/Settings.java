package com.cloudera.integration.oracle.goldengate.ldv.common;

import com.cloudera.integration.oracle.goldengate.ldv.common.io.AsciiLengthFormatter;
import com.cloudera.integration.oracle.goldengate.ldv.common.io.LengthFormatter;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by jcustenborder on 4/24/15.
 */
public class Settings {
    public enum LengthEncoding {
        ASCII,
        BINARY
    }

    int fieldLength;
    LengthEncoding fieldLengthEncoding;
    int recordLength;
    LengthEncoding recordLengthEncoding;
    String[] metadataColumns;
    Character fieldFlagNull='N';
    Character fieldFlagMissing='M';
    Character fieldFlagPresent='P';

    public LengthEncoding getRecordLengthEncoding() {
        return recordLengthEncoding;
    }

    public void setRecordLengthEncoding(LengthEncoding recordLengthEncoding) {
        this.recordLengthEncoding = recordLengthEncoding;
    }

    public LengthEncoding getFieldLengthEncoding() {
        return fieldLengthEncoding;
    }

    public void setFieldLengthEncoding(LengthEncoding fieldLengthEncoding) {
        this.fieldLengthEncoding = fieldLengthEncoding;
    }

    public String[] getMetadataColumns() {
        return metadataColumns;
    }

    public void setMetadataColumns(String[] metadataColumns) {
        this.metadataColumns = metadataColumns;
    }

    public int getRecordLength() {
        return recordLength;
    }

    public void setRecordLength(int recordLength) {
        this.recordLength = recordLength;
    }

    public Character getFieldFlagMissing() {
        return fieldFlagMissing;
    }

    public void setFieldFlagMissing(Character fieldFlagMissing) {
        this.fieldFlagMissing = fieldFlagMissing;
    }

    public Character getFieldFlagNull() {
        return fieldFlagNull;
    }

    public void setFieldFlagNull(Character fieldFlagNull) {
        this.fieldFlagNull = fieldFlagNull;
    }

    public Character getFieldFlagPresent() {
        return fieldFlagPresent;
    }

    public void setFieldFlagPresent(Character fieldFlagPresent) {
        this.fieldFlagPresent = fieldFlagPresent;
    }

    public int getFieldLength() {
        return fieldLength;
    }

    public void setFieldLength(int fieldLength) {
        this.fieldLength = fieldLength;
    }

    LengthFormatter getFieldLengthFormatter(){
        if(LengthEncoding.ASCII == this.fieldLengthEncoding){
            return new AsciiLengthFormatter(this.fieldLength);
        }
        throw new UnsupportedOperationException(this.fieldLengthEncoding + " is not supported");
    }

    LengthFormatter getRecordLengthFormatter(){
        if(LengthEncoding.ASCII == this.recordLengthEncoding){
            return new AsciiLengthFormatter(this.recordLength);
        }
        throw new UnsupportedOperationException(this.recordLengthEncoding + " is not supported");
    }

    Map<Character, FieldFlag> getCharToFieldFlagLookup() {
        Map<Character, FieldFlag> fieldFlagLookup = new HashMap<>();
        fieldFlagLookup.put(fieldFlagMissing, FieldFlag.MISSING);
        fieldFlagLookup.put(fieldFlagNull, FieldFlag.NULL);
        fieldFlagLookup.put(fieldFlagPresent, FieldFlag.PRESENT);
        return fieldFlagLookup;
    }

    Map<FieldFlag, Integer> getFieldFlagToCharLookup(){
        Map<FieldFlag, Integer> lookup = new HashMap<>();
        lookup.put(FieldFlag.MISSING,  (int) fieldFlagMissing.charValue());
        lookup.put(FieldFlag.NULL,  (int) fieldFlagNull.charValue());
        lookup.put(FieldFlag.PRESENT,  (int) fieldFlagPresent.charValue());
        return lookup;
    }
}
