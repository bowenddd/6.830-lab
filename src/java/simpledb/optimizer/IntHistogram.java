package simpledb.optimizer;

import simpledb.execution.Operator;
import simpledb.execution.Predicate;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    // User defined variable
    private int buckets;

    private int min;

    private int max;

    private double []bucketRange;

    private int []counts;

    private int []groups;

    private int tupCounts;


    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
        this.buckets = buckets;
        this.min = min;
        this.max = max;
        this.counts = new int[buckets];
        this.bucketRange = new double[buckets];
        this.groups = new int[max-min+1];
        this.tupCounts = 0;
        double rangVal = (double)(max-min+1)/buckets;
        for(int i = 0 ; i < buckets ; i++){
            this.bucketRange[i] = (i+1)*rangVal+min;
        }
        for(int i = min ; i <= max ; i++){
            this.groups[i-min] = getGroup(i);
        }
    }

    private int getGroup(int v){
        for(int i = 0 ; i < this.buckets ; i++){
            if (v < this.bucketRange[i]){
                return i;
            }
        }
        return this.buckets-1;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        int group = this.groups[v-min];
        this.counts[group]++;
        this.tupCounts++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {

    	// some code goes here
        if(zeroProb(op,v)){
            return 0.0;
        }
        if(oneProb(op,v)){
            return 1.0;
        }
        int group = this.groups[v-min];
        double right = this.bucketRange[group];
        double left = group == 0 ? this.min : this.bucketRange[group-1];
        if(Predicate.Op.NOT_EQUALS.equals(op)){
            return 1-this.counts[group]/(right-left)/this.tupCounts;
        }
        if(Predicate.Op.EQUALS.equals(op)){
            return this.counts[group]/(right-left)/this.tupCounts;
        }
        if(Predicate.Op.GREATER_THAN.equals(op)||Predicate.Op.GREATER_THAN_OR_EQ.equals(op)){
            double res = 0;
            int tups = 0;
            res +=(right-v)/(right-left)*this.counts[group]/this.tupCounts;
            if (right-v < 1e-10){
                res += (right-left)*this.counts[group]/this.tupCounts;
            }
            for(int i = group+1; i < this.buckets ; i++){
                tups += this.counts[i];
            }
            res += (double)tups/this.tupCounts;
            return res;
        }
        if(Predicate.Op.LESS_THAN.equals(op)||Predicate.Op.LESS_THAN_OR_EQ.equals(op)){
            double res = 0;
            int tups = 0;
            res += (v-left)/(right-left)*this.counts[group]/this.tupCounts;
            if (v - left < 1e-10){
                res += (right-left)*this.counts[group]/this.tupCounts;
            }
            for(int i = group-1; i >=0 ; i--){
                tups += this.counts[i];
            }
            res += (double)tups/this.tupCounts;
            return res;
        }
        return -1.0;
    }

    private boolean zeroProb(Predicate.Op op, int v){
        if ((Predicate.Op.LESS_THAN.equals(op)||Predicate.Op.LESS_THAN_OR_EQ.equals(op))
                && v <= min){
            return true;
        }
        if((Predicate.Op.GREATER_THAN.equals(op)||Predicate.Op.GREATER_THAN_OR_EQ.equals(op))
                && v >= max){
            return true;
        }
        return false;
    }
    private boolean oneProb(Predicate.Op op, int v){
        if ((Predicate.Op.LESS_THAN.equals(op)||Predicate.Op.LESS_THAN_OR_EQ.equals(op))
                && v >= max){
            return true;
        }
        if((Predicate.Op.GREATER_THAN.equals(op)||Predicate.Op.GREATER_THAN_OR_EQ.equals(op))
                && v <= min){
            return true;
        }
        if(Predicate.Op.NOT_EQUALS.equals(op) && (v < min || v > max)){
            return true;
        }
        return false;
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        return String.format("Information of This IntHistogram:\n" +
                "min %d, max: %d buckets %d\n",min,max,buckets);
    }
}
