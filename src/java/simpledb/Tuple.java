package simpledb;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;

    private TupleDesc tuple_desc;
    private Field[] tuple_fields;
    private RecordId rid;

    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        if(!(td instanceof TupleDesc) || td.numFields() <= 0){
            throw new IllegalArgumentException("Incorrect schema or field count for tuple");
        }
        this.tuple_desc = td;
        this.tuple_fields = new Field[td.numFields()];
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        return (tuple_desc);
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        return(this.rid);
    }

    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        this.rid = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field.
     */
    public void setField(int i, Field f) {
        if ( i >= tuple_fields.length || i < 0){
            throw new IllegalArgumentException("Invalid index");
        }
        tuple_fields[i] = f;
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     *
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        if ( i >= tuple_fields.length || i < 0){
            throw new IllegalArgumentException("Invalid index");
        }
        return(tuple_fields[i]); //Default null for object array.
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     *
     * column1\tcolumn2\tcolumn3\t...\tcolumnN
     *
     * where \t is any whitespace (except a newline)
     */
    public String toString() {
        String str = "";
        for(int i = 0 ; i < tuple_fields.length-1; i ++){
            str += tuple_fields[i];
            str += "\t";
        }
        str += tuple_fields[tuple_fields.length-1];
        return(str);
    }

    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields()
    {
        // fields are reference type so we get list iterator.
        Iterator<Field> it = Arrays.asList(tuple_fields).iterator();
        return(it);
    }

    /**
     * reset the TupleDesc of this tuple (only affecting the TupleDesc)
     * */
    public void resetTupleDesc(TupleDesc td)
    {
        if(!(td instanceof TupleDesc) || td.numFields() <= 0){
            throw new IllegalArgumentException("Incorrect schema or field count for tuple");
        }
        tuple_desc = td;
    }
}
