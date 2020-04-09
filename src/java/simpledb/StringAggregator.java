package simpledb;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    private int gbfield;
    private Type gbfieldType;
    private int afield;
    private Op what;
    private HashMap<Object, ArrayList<String>> agg;

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldType = gbfieldtype;
        this.afield = afield;
        this.what = what;
        this.agg = new HashMap<Object, ArrayList<String>>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field tupGBField = null;

        if(gbfield==Aggregator.NO_GROUPING){
            agg.put("",new ArrayList<>());
        } else {tupGBField = tup.getField(gbfield);} //get the field from the index

        if(gbfieldType == Type.INT_TYPE){
            Integer groupKey = ((IntField) tupGBField).getValue();
            if(!agg.containsKey(groupKey)){
                agg.put(groupKey, new ArrayList<>());
            }
            String value = ((StringField) tup.getField(afield)).getValue();
            agg.get(groupKey).add(value);

        } else{
            String groupKey = ((StringField) tupGBField).getValue();
            if(!agg.containsKey(groupKey)){
                agg.put(groupKey, new ArrayList<>());
            }
            String value = ((StringField) tup.getField(afield)).getValue();
            agg.get(groupKey).add(value);
        }
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
        return new StringIter();
//        throw new UnsupportedOperationException("please implement me for lab2");
    }

    private class StringIter implements OpIterator{
        private ArrayList<Tuple> ret = new ArrayList<Tuple>();
        private Iterator<Tuple> it;

        public StringIter(){
            for(Map.Entry<Object, ArrayList<String>> i : agg.entrySet()){
                Tuple entryTup = new Tuple(this.getTupleDesc());
                int val = 0;
                ArrayList<String> curr = i.getValue();
                String retStr = "";
                switch(what){
                    case MIN:
                        retStr = curr.get(0);
                        for(String compareVal : curr){
                            if(retStr.compareTo(compareVal)>0){
                                retStr = compareVal;
                            }
                        }
                        break;
                    case MAX:
                        retStr = curr.get(0);
                        for(String compareVal: curr){
                            if(retStr.compareTo(compareVal)<0){
                                retStr = compareVal;
                            }
                        }
                        break;
                    case SUM:
                        break;
                    case AVG:
                        break;
                    case COUNT:
                        val = i.getValue().size();
                        break;
                    case SUM_COUNT:
                        break;
                    case SC_AVG:
                        break;
                }
                if(gbfieldType!=null){
                    if(gbfieldType == Type.INT_TYPE){
                        entryTup.setField(0, new IntField((Integer) i.getKey()));
                    } else {
                        entryTup.setField(0, new StringField((String) i.getKey(), i.getKey().toString().length()));
                    }
                }
                if(what==Op.COUNT){
                    entryTup.setField(1, new IntField(val));
                } else {
                    entryTup.setField(1, new StringField(retStr, retStr.length()));
                }

                ret.add(entryTup);
            }
        }


        @Override
        public void open() throws DbException, TransactionAbortedException {
            it = ret.iterator();
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            return it.hasNext();
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            return it.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            it = ret.iterator();
        }

        @Override
        public TupleDesc getTupleDesc() {
            if(gbfield == Aggregator.NO_GROUPING){
                if(what!= Op.COUNT){
                    return new TupleDesc(new Type[]{Type.STRING_TYPE});
                } else {return new TupleDesc(new Type[]{Type.INT_TYPE});}
            } else {
                if(what!= Op.COUNT){
                    return new TupleDesc(new Type[]{gbfieldType,Type.STRING_TYPE});
                } else {return new TupleDesc(new Type[]{gbfieldType, Type.INT_TYPE});}
            }
        }

        @Override
        public void close() {
            it = null;
        }
    }
}
