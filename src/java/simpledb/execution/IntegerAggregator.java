package simpledb.execution;

import jdk.internal.org.objectweb.asm.tree.analysis.Value;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator extends Operator implements Aggregator  {

    private static final long serialVersionUID = 1L;

    // User defined variable
    private int gbfield;

    private Type gbfieldType;

    private int afield;

    private Op what;


    // used for sum,avg,max,min
    private Map<Integer, Integer> integerGroupValMap;
    private Map<String, Integer> stringGroupValMap;

    // used for avg,count
    private Map<Integer, Integer> integerGroupCountMap;
    private Map<String, Integer> stringGroupCountMap;

    // used for avg
    private Map<Integer, Integer> integerGroupAvgMap;
    private Map<String, Integer> stringGroupAvgMap;

    private TupleDesc td;


    private Iterator<Map.Entry<Integer,Integer>> integerGroupIterator;
    private Iterator<Map.Entry<String,Integer>> stringGroupIterator;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldType = gbfieldtype;
        this.afield = afield;
        this.what = what;
        this.integerGroupValMap = new HashMap<>();
        this.integerGroupCountMap = new HashMap<>();
        this.integerGroupAvgMap = new HashMap<>();
        this.stringGroupCountMap = new HashMap<>();
        this.stringGroupValMap = new HashMap<>();
        this.stringGroupAvgMap = new HashMap<>();
        if(this.gbfield == NO_GROUPING){
            this.td = new TupleDesc(new Type[]{Type.INT_TYPE});
        }else if (Type.INT_TYPE.equals(this.gbfieldType)){
            this.td = new TupleDesc(new Type[]{Type.INT_TYPE,Type.INT_TYPE});
        }else{
            this.td = new TupleDesc(new Type[]{Type.STRING_TYPE,Type.INT_TYPE});
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes hereelse{
        if(Type.STRING_TYPE.equals(this.gbfieldType)){
            mergeForStringGroup(tup);
            return;
        }
        mergeForIntegerGroup(tup);
    }

    private void mergeForStringGroup(Tuple tup){
        String group = tup.getField(this.gbfield).toString();
        int value = ((IntField)tup.getField(this.afield)).getValue();
        if(Op.MAX.equals(this.what)){
            int groupVal = this.stringGroupValMap.getOrDefault(group,value);
            this.stringGroupValMap.put(group,Math.max(groupVal,value));
            return;
        }
        if(Op.MIN.equals(this.what)){
            int groupVal = this.stringGroupValMap.getOrDefault(group,value);
            this.stringGroupValMap.put(group,Math.min(groupVal,value));
            return;
        }
        if(Op.SUM.equals(this.what)){
            int groupVal = this.stringGroupValMap.getOrDefault(group,0);
            this.stringGroupValMap.put(group,groupVal+value);
            return;
        }
        if(Op.COUNT.equals(this.what)){
            int groupCount = this.stringGroupCountMap.getOrDefault(group,0);
            this.stringGroupCountMap.put(group,groupCount+1);
            return;
        }
        if(Op.AVG.equals(this.what)){
            int groupVal = this.stringGroupValMap.getOrDefault(group,0);
            this.stringGroupValMap.put(group,groupVal+value);
            int groupCount = this.stringGroupCountMap.getOrDefault(group,0);
            this.stringGroupCountMap.put(group,groupCount+1);
        }
    }

    private void mergeForIntegerGroup(Tuple tup){
        int group = NO_GROUPING;
        if(this.gbfield!=NO_GROUPING){
            group = ((IntField)tup.getField(this.gbfield)).getValue();
        }
        int value = ((IntField)tup.getField(this.afield)).getValue();
        if(Op.MAX.equals(this.what)){
            int groupVal = this.integerGroupValMap.getOrDefault(group,value);
            this.integerGroupValMap.put(group,Math.max(groupVal,value));
            return;
        }
        if(Op.MIN.equals(this.what)){
            int groupVal = this.integerGroupValMap.getOrDefault(group,value);
            this.integerGroupValMap.put(group,Math.min(groupVal,value));
            return;
        }
        if(Op.SUM.equals(this.what)){
            int groupVal = this.integerGroupValMap.getOrDefault(group,0);
            this.integerGroupValMap.put(group,groupVal+value);
            return;
        }
        if(Op.COUNT.equals(this.what)){
            int groupCount = this.integerGroupCountMap.getOrDefault(group,0);
            this.integerGroupCountMap.put(group,groupCount+1);
            return;
        }
        if(Op.AVG.equals(this.what)){
            int groupVal = this.integerGroupValMap.getOrDefault(group,0);
            this.integerGroupValMap.put(group,groupVal+value);
            int groupCount = this.integerGroupCountMap.getOrDefault(group,0);
            this.integerGroupCountMap.put(group,groupCount+1);
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        return this;
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        super.open();
        if(Op.COUNT.equals(this.what)){
            this.integerGroupIterator = this.integerGroupCountMap.entrySet().iterator();
            this.stringGroupIterator = this.stringGroupCountMap.entrySet().iterator();
            return;
        }
        if(Op.AVG.equals(this.what)){
            for(int key : this.integerGroupCountMap.keySet()){
                this.integerGroupAvgMap.put(key,this.integerGroupValMap.get(key)/this.integerGroupCountMap.get(key));
            }
            for(String key : this.stringGroupCountMap.keySet()){
                this.stringGroupAvgMap.put(key,this.stringGroupValMap.get(key)/this.stringGroupCountMap.get(key));
            }
            this.integerGroupIterator = this.integerGroupAvgMap.entrySet().iterator();
            this.stringGroupIterator = this.stringGroupAvgMap.entrySet().iterator();
            return;
        }
        this.integerGroupIterator = this.integerGroupValMap.entrySet().iterator();
        this.stringGroupIterator = this.stringGroupValMap.entrySet().iterator();
    }

    @Override
    public void close() {
        super.close();
        this.integerGroupIterator = null;
        this.stringGroupIterator = null;
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        this.close();
        this.open();
    }

    @Override
    protected Tuple fetchNext() throws DbException, TransactionAbortedException {
        if(Type.STRING_TYPE.equals(this.gbfieldType)){
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
            if(this.gbfield==NO_GROUPING){
                tuple.setField(0,new IntField(entry.getValue()));
            }else{
                tuple.setField(0,new IntField(entry.getKey()));
                tuple.setField(1,new IntField(entry.getValue()));
            }
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
