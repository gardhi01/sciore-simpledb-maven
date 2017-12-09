package simpledb.query;

import java.util.*;

/**
 * The scan class corresponding to the <i>project</i> relational
 * algebra operator.
 * All methods except hasField delegate their work to the
 * underlying scan.
 * @author Edward Sciore
 */
public class JoinScan implements Scan {
   private Scan prod;
   private Predicate pred;
   
   /**
    * Creates a project scan having the specified
    * underlying scan and field list.
    * @param s1 
    * @param s2
    * @param pred 
    */
   public JoinScan(Scan s1, Scan s2, Predicate pred) {
      this.prod = new ProductScan(s1, s2);
      this.pred = pred;
      prod.next();
   }
   
   public void beforeFirst() {
      prod.beforeFirst();
   }
   
   public boolean next() {
      while (prod.next()) { 
          if (pred.isSatisfied(prod)) {
              return true;
          }
      }
      return false;
   }
   
   public void close() {
      prod.close();
   }
   
   public Constant getVal(String fldname) {
       return prod.getVal(fldname);
   }
   
   public int getInt(String fldname) {
       return prod.getInt(fldname);
   }
   
   public String getString(String fldname) {
       return prod.getString(fldname);
   }
   
   /**
    * Returns true if the specified field
    * is in the projection list.
    * @see simpledb.query.Scan#hasField(java.lang.String)
    */
   public boolean hasField(String fldname) {
      return prod.hasField(fldname);
   }
}
