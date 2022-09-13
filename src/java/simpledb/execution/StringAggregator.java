package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.IntField;
import simpledb.storage.StringField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator extends Operator implements Aggregator {

    private static final long serialVersionUID = 1L;

    // User defined variable

    private int gbfield;

    private Type gbfieldtype;

    private int afield;

    private Op what;

    private Map<String,Integer> stringGroupMap;

    private Map<Integer,Integer> integerGroupMap;

    private TupleDesc td;

    private Iterator<Map.Entry<String, Integer>> stringGroupIterator;

    private Iterator<Map.Entry<Integer, Integer>> integerGroupIterator;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) throws IllegalArgumentException {
        // some code goes here
        if (!Op.COUNT.equals(what)){
            throw new IllegalArgumentException();
        }
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        this.integerGroupMap = new HashMap<>();
        this.stringGroupMap = new HashMap<>();
        if(gbfield==NO_GROUPING){
            this.td = new TupleDesc(new Type[]{Type.INT_TYPE});
        }else if (Type.INT_TYPE.equals(gbfieldtype)){
            this.td = new TupleDesc(new Type[]{Type.INT_TYPE,Type.INT_TYPE});
        }else{
            this.td = new TupleDesc(new Type[]{Type.STRING_TYPE,Type.INT_TYPE});
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        if(Type.STRING_TYPE.equals(this.gbfieldtype)){
            mergeForStringGroup(tup);
            return;
        }
        mergeForIntegerGroup(tup);
    }

    private void mergeForStringGroup(Tuple tup){
        String group = tup.getField(this.gbfield).toString();
        Integer value = this.stringGroupMap.getOrDefault(group, 0);
        this.stringGroupMap.put(group,value+1);
    }

    private void mergeForIntegerGroup(Tuple tup){
        int group = NO_GROUPING;
        if(!(this.gbfield == NO_GROUPING)){
            group = ((IntField)tup.getField(this.gbfield)).getValue();
        }
        Integer value = this.integerGroupMap.getOrDefault(group,0);
        this.integerGroupMap.put(group,value+1);
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        return this;
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        super.open();
        this.stringGroupIterator = this.stringGroupMap.entrySet().iterator();
        this.integerGroupIterator = this.integerGroupMap.entrySet().iterator();
    }

    @Override
    public void close() {
        super.close();
        this.stringGroupIterator = null;
        this.integerGroupIterator = null;
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        this.close();
        this.open();
    }

    @Override
    protected Tuple fetchNext() throws DbException, TransactionAbortedException {
        if(Type.STRING_TYPE.equals(this.gbfieldtype)){
            return fetchForStringGroup();
        }
        return fetchForIntegerGroup();
    }

    private Tuple fetchForStringGroup() throws DbException, TransactionAbortedException{
        while(this.stringGroupIterator.hasNext()){
            Map.Entry<String, Integer> entry = this.stringGroupIterator.next();
            Tuple tuple = new Tuple(this.td);
            tuple.setField(0,new StringField(entry.getKey(),entry.getKey().length()));
            tuple.setField(1,new IntField(entry.getValue()));
            return tuple;
        }
        return null;
    }

    private Tuple fetchForIntegerGroup() throws DbException, TransactionAbortedException{
        while(this.integerGroupIterator.hasNext()){
            Map.Entry<Integer, Integer> entry = this.integerGroupIterator.next();
            Tuple tuple = new Tuple(this.td);
            if(this.gbfield == NO_GROUPING){
                tuple.setField(0,new IntField(entry.getValue()));
                return tuple;
            }
            tuple.setField(0,new IntField(entry.getKey()));
            tuple.setField(1,new IntField(entry.getValue()));
            return tuple;
        }
        return null;
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[0];
    }

    @Override
    public void setChildren(OpIterator[] children) {

    }

    @Override
    public TupleDesc getTupleDesc() {
        return this.td;
    }
}
