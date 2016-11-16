package org.pentaho.di.core.sql.visitors;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.pentaho.di.core.Condition;
import org.pentaho.di.core.exception.KettleSQLException;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.row.ValueMetaAndData;

public class WhereVisitor extends SqlBasicVisitor<Condition> {

  public Condition visit( SqlIdentifier id ) {
    assert id.isSimple() : "Only supporting simple field references in WHERE clause in POC";
    return new Condition( id.getSimple(), Condition.OPERATOR_NONE, null, null );
  }

  public Condition visit( SqlLiteral literal ) {
    try {
      Object value;
      switch ( literal.getTypeName().getFamily() ) {
        case CHARACTER:
          value = literal.toValue();
          break;
        default:
          value = SqlLiteral.value( literal );

      }
      return new Condition( null, Condition.OPERATOR_NONE, null,
        new ValueMetaAndData( "literal", value ) );
    } catch ( KettleValueException e ) {
      throw new RuntimeException( e );
    }
  }

  public Condition visit( SqlCall call ) {
    switch ( call.getKind() ) {
      case AND:
      case OR:
        assert call.getOperandList().size() == 2;  // binary ops
        Condition compound = new Condition();
        for ( SqlNode node : call.getOperandList() ) {
          compound.addCondition( node.accept( this ) );
        }
        compound.getChildren().get( 1 ).setOperator( Condition.getOperator( call.getKind().name() ) );
        return compound;
      default:
        return getAtomicCondition( call );
    }
  }

  public Condition visit( SqlNodeList list ) {
    Condition cond = new Condition();
    for ( SqlNode node : list ) {
      cond = mergeList( cond, node.accept( this ) );
    }
    return cond;
  }

  private Condition getAtomicCondition( SqlCall call ) {
    assert call.getOperandList().size() == 2;
    try {
      Condition cond = mergeConditions(
        call.getOperandList().get( 0 ).accept( this ),
        call.getOperandList().get( 1 ).accept( this ) );
      cond.setFunction( kettleType( call.getKind() ) );
      return cond;
    } catch ( KettleSQLException e ) {
      throw new RuntimeException( e );
    }
  }

  private Condition mergeConditions( Condition left, Condition right ) {
    return new Condition( left.getLeftValuename(), left.getFunction(),
      right.getRightValuename(), right.getRightExact() );
  }

  private Condition mergeList( Condition first, Condition next ) {
    try {
      return new Condition(
        null, Condition.FUNC_EQUAL, null,
        new ValueMetaAndData( "constant-in-list",
          concatLists( first, next ) )
      );
    } catch ( KettleValueException e ) {
      throw new RuntimeException( e );
    }
  }

  private Object concatLists( Condition first, Condition next ) {
    String left = first.getRightExactString() == null ? ""
      : first.getRightExactString() + ";";
    String right = next.getRightExactString() == null ? ""
      : next.getRightExactString();
    return left + right;
  }

  private int kettleType( SqlKind kind ) throws KettleSQLException {
    switch ( kind ) {
      case EQUALS:
        return Condition.FUNC_EQUAL;
      case NOT_EQUALS:
        return Condition.FUNC_NOT_EQUAL;
      case GREATER_THAN:
        return Condition.FUNC_LARGER;
      case GREATER_THAN_OR_EQUAL:
        return Condition.FUNC_LARGER_EQUAL;
      case LESS_THAN:
        return Condition.FUNC_SMALLER;
      case LESS_THAN_OR_EQUAL:
        return Condition.FUNC_SMALLER_EQUAL;
      case IN:
        return Condition.FUNC_IN_LIST;
      case LIKE:
        return Condition.FUNC_LIKE;
      default:
        throw new KettleSQLException( "No mapping for SqlKind " + kind );
    }
  }
}
