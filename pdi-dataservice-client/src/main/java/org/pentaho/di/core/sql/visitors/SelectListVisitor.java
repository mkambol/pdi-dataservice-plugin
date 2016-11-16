package org.pentaho.di.core.sql.visitors;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelectKeyword;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.sql.SQLAggregation;
import org.pentaho.di.core.sql.SQLField;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class SelectListVisitor extends SqlBasicVisitor<List<SQLField>> {

  private final RowMetaInterface rowMeta;

  public SelectListVisitor( RowMetaInterface rowMeta ) {
    this.rowMeta = rowMeta;
  }

  public List<SQLField> visit( SqlNodeList nodeList ) {
    List<SQLField> sqlFields = new ArrayList<>();
    for ( SqlNode node : nodeList ) {
      sqlFields.addAll( node.accept( this ) );
    }
    return sqlFields;
  }

  public List<SQLField> visit( SqlIdentifier id ) {
    return fieldNames( id ).stream()
      .map( field ->
        new SQLField( null, field, null, null, rowMeta.searchValueMeta( field ) ) )
      .collect( Collectors.toList() );
  }

  public List<SQLField> visit( SqlCall call ) {
    List<SQLField> fields = call.getOperandList().get( 0 ).accept( this );
    switch ( call.getKind() ) {
      case AS:
        fields.get( 0 ).setAlias( call.getOperandList().get( 1 ).toString() );
        return fields;
      case COUNT:
        fields.get( 0 ).setCountDistinct( isDistinct( call ) );
      case SUM:
      case AVG:
      case MIN:
      case MAX:
        fields.get( 0 ).setAggregation( SQLAggregation.valueOf( call.getKind().name() ) );
        return fields;
    }
    return Collections.emptyList();
  }

  private boolean isDistinct( SqlCall call ) {
    return call.getFunctionQuantifier() != null
      && call.getFunctionQuantifier().getValue().equals( SqlSelectKeyword.DISTINCT );
  }

  /**
   * Retrieves all field names for '*', otherwise gets the identifier in the last component position. If present, the
   * first identifier will be the table name.
   */
  private List<String> fieldNames( SqlNode node ) {
    Preconditions.checkArgument( node.getKind() == SqlKind.IDENTIFIER );
    SqlIdentifier id = ( (SqlIdentifier) node );
    if ( id.isStar() ) {
      return rowMeta.getValueMetaList().stream()
        .map( vm -> vm.getName() )
        .collect( Collectors.toList() );
    } else {
      return ImmutableList.of( id.getComponent( id.names.size() - 1 ).getSimple() );
    }
  }


}
