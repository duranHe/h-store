package org.voltdb.compiler;

import java.util.Iterator;

import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.ColumnRef;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;

public class VerticalPartitioningCompiler {
    Database m_db = null;
    
    public VerticalPartitioningCompiler(Database db) {
        m_db = db;
    }
    
    /**
     * Hard code for now.
     * Only for tpcc benchmark.
     * Evicted columns: HISTORY: H_D_ID, H_W_ID
     */
    public void compileEvictedColumns() {
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
}
