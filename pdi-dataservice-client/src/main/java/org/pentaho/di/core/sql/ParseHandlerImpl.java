package org.pentaho.di.core.sql;

import com.google.common.base.Preconditions;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.ValidationException;
import org.apache.calcite.util.Pair;
import org.pentaho.di.core.exception.KettleSQLException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.trans.dataservice.jdbc.ThinResultSetMetaData;

import java.util.ArrayList;
import java.util.List;

public class ParseHandlerImpl implements ParseHandler {

  private static final ParseHandler INSTANCE = new ParseHandlerImpl();

  public static ParseHandler instance() {
    return INSTANCE;
  }

  private ParseHandlerImpl() {
  }

  @Override public SqlNode parse( String sql ) throws KettleSQLException {
    try {
      return SqlParser.create( sql ).parseQuery();
    } catch ( SqlParseException e ) {
      throw new KettleSQLException( e );
    }
  }

  @Override public SqlNode validate( String sql, RowMetaInterface rowMeta ) throws KettleSQLException {
    try {
      SchemaPlus schema = createSchema( sql, rowMeta );
      Planner planner = createQueryPlanner( schema );
      return planner.validate( planner.parse( sql ) );
    } catch ( SqlParseException | ValidationException e ) {
      throw new KettleSQLException( "Failed to validate sql.", e );
    }
  }

  @Override public String getTableName( SqlNode sql ) throws KettleSQLException {
    Preconditions.checkArgument( sql.getKind() != SqlKind.SELECT || sql.getKind() != SqlKind.ORDER_BY,
      "Query is not a SELECT" );
    SqlSelect select;
    if ( sql.getKind().equals( SqlKind.ORDER_BY ) ) {
      select = (SqlSelect) ( (SqlOrderBy) sql ).getOperandList().get( 0 );
    } else {
      select = (SqlSelect) sql;
    }
    if ( select.getFrom().getKind() == SqlKind.IDENTIFIER ) {
      SqlIdentifier identifier = (SqlIdentifier) select.getFrom();
      return identifier.getComponent( 0 ).toString();
    }
    throw new KettleSQLException( "Only single table data service queries are suppoerted" );
  }


  private Planner createQueryPlanner( SchemaPlus schema ) {
    Frameworks.ConfigBuilder configBuilder = Frameworks.newConfigBuilder();
    configBuilder.defaultSchema( schema );
    FrameworkConfig fwConfig = configBuilder.build();
    SqlParser.ConfigBuilder parserConfig = SqlParser.configBuilder( fwConfig.getParserConfig() );

    parserConfig.setCaseSensitive( false );
    parserConfig.setConfig( parserConfig.build() );
    return Frameworks.getPlanner( fwConfig );
  }

  private SchemaPlus createSchema( String sql, RowMetaInterface rowMeta ) throws KettleSQLException {
    String tableName = getTableName( parse( sql ) );
    SchemaPlus rootSchema = Frameworks.createRootSchema( true );

    rootSchema.add( tableName, new AbstractTable() {
      @Override public RelDataType getRowType( RelDataTypeFactory relDataTypeFactory ) {
        final List<Pair<String, RelDataType>> columnDesc = new ArrayList<>();
        for ( ValueMetaInterface valueMeta : rowMeta.getValueMetaList() ) {
          Class javaType = ThinResultSetMetaData.getClassForType( valueMeta.getType() ).orElse( String.class );
          columnDesc.add( Pair.of( valueMeta.getName().toUpperCase(),
            relDataTypeFactory.createJavaType( javaType ) ) );
        }
        return relDataTypeFactory.createStructType( columnDesc );
      }
    } );
    return rootSchema;
  }

}
