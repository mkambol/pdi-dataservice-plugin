package org.pentaho.di.core.sql;

import org.pentaho.di.core.exception.KettleSQLException;
import org.pentaho.di.core.row.RowMetaInterface;

public interface SqlToDSQueryConverter {
  SQL convert( String sql, RowMetaInterface rowMeta ) throws KettleSQLException;
}
