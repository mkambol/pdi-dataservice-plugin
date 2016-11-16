package org.pentaho.di.core.sql;

import org.apache.calcite.sql.SqlNode;
import org.pentaho.di.core.exception.KettleSQLException;
import org.pentaho.di.core.row.RowMetaInterface;

public interface ParseHandler {

  SqlNode parse( String sql ) throws KettleSQLException;

  SqlNode validate( String sql, RowMetaInterface rowMeta ) throws KettleSQLException;

  String getTableName( SqlNode node ) throws KettleSQLException;

}
