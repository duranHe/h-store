package org.voltdb.compiler;

import java.util.Iterator;

import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.CatalogMap;
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
            if(table.getName().equalsIgnoreCase("history")) {
                table.setEvictable(true);
                table.getEvictcolumns().add("H_D_ID");
                table.getEvictcolumns().add("H_W_ID");
            }
        }
    }
}
