package org.voltdb.compiler;

import java.util.*;
import java.util.Map.Entry;

import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.ColumnRef;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.Table;

import edu.brown.catalog.CatalogUtil;

public class VerticalPartitioningCompiler {
    static Database m_db = null;
    static HashMap<Table, HashMap<Column, Integer>> columnCounter;
    static int evictedColumnCount = 2; // FIX ME
    static double evictedColumnPercentage = 0.3; // FIX ME
    
    /*
     *  0: num of column
     *  1: percentage of column
     *  2: based on workload (hardcode)
     */
    static int evictedColumnModel = 0;
    
    public static void setup(Database db) {
        m_db = db;
        columnCounter = new HashMap<Table, HashMap<Column, Integer>>();
        
        // setup column counter for tables that are marked evictable
        CatalogMap<Table> tables = m_db.getTables();
        Iterator<Table> ite = tables.iterator();
        while(ite.hasNext()) {
            Table table = ite.next();
            
            // for test
//            if(table.getName().equalsIgnoreCase("order_line")) {
//                table.setEvictable(true);
//            }
            
            // only set up evicted columns for EvictedTable
            if(table.getEvictable()) {
                HashMap<Column, Integer> counter = new HashMap<Column, Integer>();
                Iterator<Column> columnIte = table.getColumns().iterator();
                while(columnIte.hasNext()) {
                    Column col = columnIte.next();
                    counter.put(col, 0);
                }
                
                columnCounter.put(table, counter);
            }
        }
    }
    
    /**
     * Workload based evictedColumn selection.
     * Hard code for now.
     * Only for tpcc benchmark.
     */
    public static void hardcode() {
        CatalogMap<Table> tableList = m_db.getTables();
        Iterator<Table> tableIte = tableList.iterator();
        
        while(tableIte.hasNext()) {
            Table table = tableIte.next();
            if(table.getName().equalsIgnoreCase("ORDER_LINE")) {
                table.setEvictable(true);
                CatalogMap<ColumnRef> evictedColumn = table.getEvictcolumns();
                
                String[] evictedColumnName = {"OL_NUMBER", "OL_W_ID"};
                
                for(int i = 0; i < evictedColumnName.length; i++){
                    String colName = evictedColumnName[i];
                    ColumnRef colRef = evictedColumn.add(colName);
                    colRef.setColumn(table.getColumns().get(colName));
                    System.out.println("type name: " + colRef.getTypeName());   
                    System.out.println("ColumnRef index: " + colRef.getIndex());
                    System.out.println("Column index: " + colRef.getColumn().getIndex());
                    System.out.println("column type: " + colRef.getColumn().getType());
                }
            }
        }
    }
    
    public static void compileEvictedColumns() {
        if(columnCounter.size() == 0) {
            return;
        }
        
        columnRefCount();
        
        for(Table table : columnCounter.keySet()) {
            ArrayList<Column> topCol = sortMap(columnCounter.get(table));
            CatalogMap<ColumnRef> evictedColumns = table.getEvictcolumns();
            System.out.println("Evictable: " + table.getName());
            
            for(Column col : topCol) {
                String colName = col.getName();
                ColumnRef colRef = evictedColumns.add(colName);
                colRef.setColumn(col);
                System.out.print(col.getName() + "; ");
            }
            System.out.println();
        }
    }
    
    /**
     * Sort the columns by "hotness"
     * @param counter
     * @return only the requested top-n columns
     */
    private static ArrayList<Column> sortMap(HashMap<Column, Integer> counter) {
        List<Map.Entry<Column, Integer>> list = 
                new ArrayList<Map.Entry<Column, Integer>>(counter.entrySet());
        
        Collections.sort(list, new Comparator<Map.Entry<Column, Integer>>() {

            @Override
            public int compare(Entry<Column, Integer> o1, Entry<Column, Integer> o2) {
                // TODO Auto-generated method stub
                return o2.getValue() - o1.getValue();
            }
        });
        
        int topN = 0;
        if(evictedColumnModel == 0) {
        	topN = evictedColumnCount;
        } else if(evictedColumnModel == 1) {
        	topN = (int) (counter.size() * evictedColumnPercentage);
        } else {
        	// shouldn't be here
        }
        
        ArrayList<Column> top = new ArrayList<Column>();
        for(int i = 0; i < list.size() && i < topN; i++) {
            Column col = list.get(i).getKey();
            int count = list.get(i).getValue();
            
            if(count != 0){
                top.add(col);
            } else {
            	// already a cold column
            	// stop even though we don't get the topN columns
                break;
            }
        }
        
        return top;
    }
    
    private static void columnRefCount() {
        CatalogMap<Procedure> procs = m_db.getProcedures();
        Iterator<Procedure> ite = procs.iterator();
        while(ite.hasNext()) {
            Procedure proc = ite.next();
            Collection<Column> columns = null;
            try {
                columns = CatalogUtil.getReferencedColumns(proc);
            } catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            
            // update the counter for all referenced columns
            for(Column col : columns) {
                for(Table t : columnCounter.keySet()) {
                    
                    HashMap<Column, Integer> counter = columnCounter.get(t);
                    if(counter.containsKey(col)) {
                        counter.put(col, counter.get(col) + 1);
                    }
                }
            }
        }
    }
}