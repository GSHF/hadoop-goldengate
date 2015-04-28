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
    Character getFieldFlagPresent='P';

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
        fieldFlagLookup.put(getFieldFlagPresent, FieldFlag.PRESENT);
        return fieldFlagLookup;
    }

    Map<FieldFlag, Integer> getFieldFlagToCharLookup(){
        Map<FieldFlag, Integer> lookup = new HashMap<>();
        lookup.put(FieldFlag.MISSING,  (int) fieldFlagMissing.charValue());
        lookup.put(FieldFlag.NULL,  (int) fieldFlagNull.charValue());
        lookup.put(FieldFlag.PRESENT,  (int) getFieldFlagPresent.charValue());
        return lookup;
    }
}
