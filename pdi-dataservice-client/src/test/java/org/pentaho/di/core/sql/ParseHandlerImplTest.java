package org.pentaho.di.core.sql;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.pentaho.di.core.exception.KettleSQLException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaInteger;
import org.pentaho.di.core.row.value.ValueMetaNumber;
import org.pentaho.di.core.row.value.ValueMetaString;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;
import static org.mockito.Mockito.when;

@RunWith( MockitoJUnitRunner.class )
public class ParseHandlerImplTest {

  @Mock RowMetaInterface rowMeta;

  List<ValueMetaInterface> valueMetaList = new ArrayList<>();

  @Before
  public void before() {
    when( rowMeta.getValueMetaList() ).thenReturn( valueMetaList );
  }

  @Test
  public void testSimpleStringTypes() throws KettleSQLException {
    valueMetaList.add( new ValueMetaString( "foo" ) );
    valueMetaList.add( new ValueMetaString( "bar" ) );
    SqlNode sqlNode = ParseHandlerImpl.instance().validate( "select foo, bar from SERVICE", rowMeta );
    assertThat( sqlNode, instanceOf( SqlSelect.class ) );
    SqlSelect select = (SqlSelect) sqlNode;
    assertThat( select.hasWhere(), is( false ) );
    assertThat( select.getSelectList().size(), is( 2 ) );
  }

  @Test
  public void testUnknownField() throws KettleSQLException {
    valueMetaList.add( new ValueMetaInteger( "foo" ) );
    valueMetaList.add( new ValueMetaString( "bar" ) );
    try {
      ParseHandlerImpl.instance().validate( "select foo, bar, baz from SERVICE ", rowMeta );
      fail( "Expected Exception");
    } catch ( Exception e ) {
      assertThat( e, instanceOf( KettleSQLException.class ) );
    }
  }

  @Test
  public void testNormalize() throws KettleSQLException {
    valueMetaList.add( new ValueMetaInteger( "WHERE" ) );
    valueMetaList.add( new ValueMetaNumber( "FROM" ) );
    SqlNode sqlNode = ParseHandlerImpl
      .instance().validate( "select \"WHERE\" from SERVICE where \"WHERE\" = \"FROM\"", rowMeta );
    System.out.println( sqlNode.toSqlString( null, true ));
//    assertThat(
//      sqlNode.toSqlString( null, true ),
//      equalTo("SELECT `SERVICE`.`WHERE`\n"
//        + "FROM `SERVICE` AS `SERVICE`\n"
//        + "WHERE (`SERVICE`.`WHERE` = `SERVICE`.`FROM`)") );

  }

}