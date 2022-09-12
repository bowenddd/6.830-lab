package simpledb.storage;

import simpledb.common.Type;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    // User defined member variables

    // save all information of fields in a tuple
    private TDItem[] tdItems;

    /**
     * A help class to facilitate organizing the information of each field
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
    }

    // get The tdItems
    private TDItem[] getTdItems(){
        return this.tdItems;
    }
    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return Arrays.stream(this.getTdItems()).iterator();
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
        // some code goes here
        int numFields = typeAr.length;
        this.tdItems = new TDItem[numFields];
        for(int i = 0 ; i < numFields ; i++){
            this.tdItems[i] = new TDItem(typeAr[i],fieldAr[i]);
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
        int numFields = typeAr.length;
        this.tdItems = new TDItem[numFields];
        for(int i = 0 ; i < numFields ; i++){
            this.tdItems[i] = new TDItem(typeAr[i],null);
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return this.getTdItems().length;
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
        // some code goes here
        if (i < 0 || i > this.numFields() || this.tdItems[i] == null){
            throw new NoSuchElementException();
        }
        if (this.tdItems[i].fieldName == null){
            return "null";
        }
        return this.tdItems[i].fieldName;
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
        // some code goes here
        if (i < 0 || i > this.numFields() || this.tdItems[i] == null || this.tdItems[i].fieldType==null){
            throw new NoSuchElementException();
        }
        return this.tdItems[i].fieldType;
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
        // some code goes here
        for(int i = 0 ; i < this.numFields() ; i++){
            if(this.getFieldName(i).equals(name)){
                return i;
            }
        }
        throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        int bytes = 0;
        for(TDItem item : this.getTdItems()){
            switch (item.fieldType){
                case INT_TYPE:
                    bytes += Type.INT_TYPE.getLen();
                    break;
                case STRING_TYPE:
                    bytes += Type.STRING_TYPE.getLen();
                    break;
            }
        }
        return bytes;
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
        // some code goes here
        int td1Nums = td1.numFields();
        int td2Nums = td2.numFields();
        Type[] types = new Type[td1Nums+td2Nums];
        String[] names = new String[td1Nums+td2Nums];
        for(int i = 0 ; i < td1Nums ; i++){
            types[i] = td1.getFieldType(i);
            names[i] = td1.getFieldName(i);
        }
        for(int i = 0 ; i < td2Nums ; i++){
            types[td1Nums+i] = td2.getFieldType(i);
            names[td1Nums+i] = td2.getFieldName(i);
        }
        return new TupleDesc(types,names);
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
        // some code goes here
        if(this == o){
            return true;
        }
        if(!(o instanceof TupleDesc)){
            return false;
        }
        TupleDesc cmptd = (TupleDesc) o;
        if(this.numFields() != cmptd.numFields()){
            return false;
        }
        for(int i = 0 ; i < this.numFields() ; i++){
            if(this.getFieldType(i) != cmptd.getFieldType(i)){
                return false;
            }
        }
        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        int hashCode = 0;
        for(int i = 0 ; i < this.numFields() ; i++){
            hashCode += ((i << 2) + 1) * (this.getFieldType(i).hashCode() >> 4);
        }
        return hashCode;
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
        StringBuilder sb = new StringBuilder();
        for(int i = 0 ; i < this.numFields() ; i++){
            sb.append(this.tdItems[i].toString());
            if(i < this.numFields() - 1){
                sb.append(',');
            }
        }
        return sb.toString();
    }
}
