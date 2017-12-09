/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package simpledb.query;

import java.util.Collection;
import simpledb.record.Schema;

/**
 *
 * @author gardhi01
 */
public class JoinPlan implements Plan {
    private Plan p1;
    private Plan p2;
    private Predicate pred;
    private Schema schema = new Schema();

    /**
    * Creates a new project node in the query tree,
    * having the specified subquery and field list.
    * @param p1 the subquery
    * @param p2
    * @param pred 
    */
    
  public JoinPlan(Plan p1, Plan p2, Predicate pred) {
      this.p1 = p1;
      this.p2 = p2;
      this.pred = pred;
   }
  
  public Scan open() {
      Scan s1 = p1.open();
      Scan s2 = p2.open();
      return new JoinScan(s1, s2, pred);
   }

    public int blocksAccessed() {
        return p1.blocksAccessed() + (p1.recordsOutput() * p2.blocksAccessed());
    }  
    
    public int recordsOutput() {
        return p1.recordsOutput();
    }
    
    public int distinctValues(String fldname) {
      return p1.distinctValues(fldname) + p2.distinctValues(fldname);
   }
   
   /**
    * Returns the schema of the projection,
    * which is taken from the field list.
    * @see simpledb.query.Plan#schema()
    */
   public Schema schema() {
      return schema;
   }
}
