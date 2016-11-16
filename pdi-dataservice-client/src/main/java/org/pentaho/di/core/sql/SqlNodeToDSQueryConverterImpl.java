package org.pentaho.di.core.sql;

import com.google.common.base.Preconditions;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.util.SqlVisitor;
import org.pentaho.di.core.Condition;
import org.pentaho.di.core.exception.KettleSQLException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.sql.visitors.SelectListVisitor;
import org.pentaho.di.core.sql.visitors.WhereVisitor;

import java.util.List;


public class SqlNodeToDSQueryConverterImpl implements SqlToDSQueryConverter {

  private final ParseHandler parseHandler;

  public SqlNodeToDSQueryConverterImpl( ParseHandler parseHandler ) {
    this.parseHandler = parseHandler;
  }


  @Override public SQL convert( String sql, RowMetaInterface rowMeta ) throws KettleSQLException {
    SqlNode sqlNode = parseHandler.parse( sql ); //parseHandler.validate( sql, rowMeta );

    return applyQueryClauses( sqlNode, rowMeta );
  }

  private SQL applyQueryClauses( SqlNode sqlNode, RowMetaInterface rowMeta ) throws KettleSQLException {
    Preconditions.checkArgument( sqlNode.getKind() == SqlKind.SELECT );
    SqlSelect select = (SqlSelect) sqlNode;

    List<SQLField> sqlFields = (List<SQLField>) select.getSelectList().accept(
      (SqlVisitor) new SelectListVisitor( rowMeta ) );
    Condition condition = select.hasWhere() ? select.getWhere().accept( new WhereVisitor() )
      : new Condition();

    return SQL.builder()
      .withSelectFields( new SQLFields( rowMeta, sqlFields ) )
      .withSqlString( sqlNode.toSqlString( SqlDialect.CALCITE ).getSql() )
      .withWhereCondition( new SQLCondition( condition ) )
      .withServiceName( parseHandler.getTableName( sqlNode ) )
      .withRowMeta( rowMeta )
      .build();
  }
}
