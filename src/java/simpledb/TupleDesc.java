package simpledb;

//import com.sun.tools.javac.util.ArrayUtils;
//import sun.security.util.ArrayUtil;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A helper class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }

        public boolean equals(Object o){
            if (o == null || !(o instanceof TDItem)){
                return false;
            }
            TDItem o_n = (TDItem) o; // Cast to TupleDesc
            if(!o_n.fieldType.equals(this.fieldType) || !o_n.fieldName.equals(this.fieldName)){
                return false;
            }
            return true;
        }
    }

    /* Since the schema is fixed, we use fieldArray */
    private TDItem[] TDItems;

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        return(Arrays.asList(TDItems).iterator());
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        if(typeAr.length != fieldAr.length){
            throw new NoSuchElementException("Invalid lengths for tupleDesc");
        }
        TDItems = new TDItem[typeAr.length];
        for (int i = 0 ; i < typeAr.length; i++){
            TDItems[i] = new TDItem(typeAr[i],fieldAr[i]);
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
        TDItems = new TDItem[typeAr.length];
        for (int i = 0 ; i < typeAr.length; i++){
            TDItems[i] = new TDItem(typeAr[i],"");
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return(TDItems.length);
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        if (i < 0 || i >= numFields()){
            throw new NoSuchElementException("Not valid field reference");
        }
        return TDItems[i].fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        if (i < 0 || i >= numFields()){
            throw new NoSuchElementException("Not valid field reference");
        }
        return TDItems[i].fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        for(int i = 0; i <TDItems.length; i++){
            if(TDItems[i].fieldName.equals(name)){
                return(i);
            }
        }
        throw new NoSuchElementException(name+ " is not a valid field name");
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        int size = 0;
        for(TDItem item : TDItems){
            size += item.fieldType.getLen();
        }
        return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        Iterator it1 = td1.iterator();
        Iterator it2 = td2.iterator();
        ArrayList<Type> typeAr = new ArrayList<>();
        ArrayList<String> fieldAr = new ArrayList<>();
        while(it1.hasNext()){
            TDItem item = (TDItem) it1.next();
            typeAr.add(item.fieldType);
            fieldAr.add(item.fieldName);
        }
        while(it2.hasNext()){
            TDItem item = (TDItem) it2.next();
            typeAr.add(item.fieldType);
            fieldAr.add(item.fieldName);
        }
        TupleDesc td3 = new TupleDesc(typeAr.toArray(new Type[0]), fieldAr.toArray(new String[0]));
        return td3;
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        if (o == null || !(o instanceof TupleDesc)){
            return false;
        }
        TupleDesc o_n = (TupleDesc) o; // Cast to TupleDesc
        if (o_n.numFields() != this.numFields()){
            return false;
        }
        Iterator<TDItem> it1 = this.iterator();
        Iterator<TDItem> it2 = o_n.iterator();
        while(it1.hasNext()){
            if(! it1.next().equals(it2.next()) ){
                return false;
            }
        }
        return true;
    }

    public int hashCode() {
        // Use string hashcode
        return(this.toString().hashCode());
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        String str = "";
        for(int i = 0; i < this.numFields(); i++){
            str += this.getFieldType(i) + "[" + i + "]";
            str += "(" + this.getFieldName(i) + "),";
        }
        return str;
    }
}
