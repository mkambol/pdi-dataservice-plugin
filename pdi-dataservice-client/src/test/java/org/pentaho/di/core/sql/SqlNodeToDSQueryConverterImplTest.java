package org.pentaho.di.core.sql;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;
import org.pentaho.di.core.exception.KettleSQLException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaInteger;
import org.pentaho.di.core.row.value.ValueMetaString;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

@RunWith( MockitoJUnitRunner.class )
public class SqlNodeToDSQueryConverterImplTest {
  SqlToDSQueryConverter converter = new SqlNodeToDSQueryConverterImpl( ParseHandlerImpl.instance() );
  RowMetaInterface rowMeta = new RowMeta();

  List<ValueMetaInterface> valueMetaList = new ArrayList<>();

  @Test
  public void testSimpleStringTypes() throws KettleSQLException {
    valueMetaList.add( new ValueMetaString( "bar" ) );
    rowMeta.addValueMeta( new ValueMetaString( "foo" ) );
    rowMeta.addValueMeta( new ValueMetaInteger( "bar" ) );
    SQL query = converter.convert( "select foo, bar as b from SERVICE", rowMeta );

//    assertThat( query, is( notNull() ) );
  }

  @Test
  public void testSimpleAgg() throws KettleSQLException {
    rowMeta.addValueMeta( new ValueMetaString( "foo" ) );
    rowMeta.addValueMeta( new ValueMetaInteger( "bar" ) );
    SQL query = converter.convert( "select sum(bar)  from SERVICE", rowMeta );
    assertThat( query.getSelectFields().getAggregateFields().size(), is( 1 ) );
    assertThat( query.getSelectFields().getAggregateFields().get( 0 ).getAggregation(), is( SQLAggregation.SUM ) );
  }

  @Test
  public void testCountDistinct() throws KettleSQLException {
    rowMeta.addValueMeta( new ValueMetaString( "foo" ) );
    rowMeta.addValueMeta( new ValueMetaInteger( "bar" ) );
    SQL query = converter.convert( "select count(distinct bar)  from SERVICE", rowMeta );
    assertThat( query.getSelectFields().getAggregateFields().size(), is( 1 ) );
    assertThat( query.getSelectFields().getAggregateFields().get( 0 ).getAggregation(), is( SQLAggregation.COUNT ) );
    assertThat( query.getSelectFields().getFields().get( 0 ).isCountDistinct(), is( true ) );
  }


  @Test
  public void testStar() throws KettleSQLException {
    rowMeta.addValueMeta( new ValueMetaString( "foo" ) );
    rowMeta.addValueMeta( new ValueMetaInteger( "bar" ) );
    SQL query = converter.convert( "select *  from SERVICE", rowMeta );
    assertThat( query.getSelectFields().getFields().size(), is( 2 ) );
  }

  @Test
  public void testSimpleWhere() throws KettleSQLException {
    rowMeta.addValueMeta( new ValueMetaString( "foo" ) );
    SQL query = converter.convert( "select *  from SERVICE where foo = 123 OR foo > 234", rowMeta );
    assertThat(
      query.getWhereCondition().getCondition().toString(),
      is( "(\n                FOO = [123]\n  OR            FOO > [234]\n)\n" ) );
  }

  @Test
  public void testNestedWhere() throws KettleSQLException {
    rowMeta.addValueMeta( new ValueMetaString( "foo" ) );
    SQL query = converter.convert( "select *  from SERVICE where (foo <> 2 AND (foo >= 1 AND (foo = 123 OR foo > 234)))", rowMeta );

    assertThat( query.getWhereCondition().getCondition().toString(),
      is( "(\n"
        + "                FOO <> [2]\n"
        + "  AND    \n"
        + "  (\n"
        + "                  FOO >= [1]\n"
        + "    AND    \n"
        + "    (\n"
        + "                    FOO = [123]\n"
        + "      OR            FOO > [234]\n"
        + "    )\n"
        + "  )\n"
        + ")\n" ) );
  }

  @Test
  public void testNestedWhereWithLike() throws KettleSQLException {
    rowMeta.addValueMeta( new ValueMetaString( "lorem_ipsum" ) );
    rowMeta.addValueMeta( new ValueMetaInteger( "wordnum" ));
    SQL query = converter.convert( "select * from lorem2 where lorem_ipsum LIKE 'vim%' OR wordnum < 5 \n"
      + "or (lorem_ipsum > 'L' and wordnum > 20)", rowMeta);
    assertThat( query.getWhereCondition().getCondition().toString(),
      is( "(\n  (\n                  LOREM_IPSUM LIKE [vim%]\n"
        + "    OR            WORDNUM < [5]\n  )\n  OR     \n  "
        + "(\n                  LOREM_IPSUM > [L]\n    AND           WORDNUM > [20]\n  )\n)\n" ) );

  }

  @Test
  public void testInList() throws KettleSQLException {
    rowMeta.addValueMeta( new ValueMetaInteger( "wordnum" ));
    SQL query = converter.convert( "select * from lorem2 where lorem_ipsum IN ('foo', 'bar')", rowMeta);
    assertThat( query.getWhereCondition().getCondition().toString(),
      is( "              LOREM_IPSUM IN LIST [foo;bar]\n" ) );
  }

  @Test
  public void testShowAnnotations() throws KettleSQLException {
    SQL query = converter.convert( "show annotations from foo", rowMeta);
  }


}