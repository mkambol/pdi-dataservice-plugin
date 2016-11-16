/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2016 by Pentaho : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.pentaho.di.core.sql;

import org.pentaho.di.core.Condition;
import org.pentaho.di.core.exception.KettleSQLException;
import org.pentaho.di.core.row.RowMetaInterface;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class SQLCondition {

  private String tableAlias;
  RowMetaInterface serviceFields;
  private Condition condition;
  private String conditionClause;
  private SQLFields selectFields;

  private static final Pattern
      PARAMETER_REGEX_PATTERN =
      Pattern.compile( "(?i)^PARAMETER\\s*\\(\\s*'(.*)'\\s*\\)\\s*=\\s*'?([^']*)'?$" );

  public SQLCondition( String tableAlias, String conditionSql, RowMetaInterface serviceFields )
      throws KettleSQLException {
    this( tableAlias, conditionSql, serviceFields, null );
  }

  public SQLCondition( String tableAlias, String conditionSql, RowMetaInterface serviceFields, SQLFields selectFields )
      throws KettleSQLException {
    this.tableAlias = tableAlias;
    this.conditionClause = conditionSql;
    this.serviceFields = serviceFields;
    this.selectFields = selectFields;

   // parse();
  }

  public SQLCondition( Condition condition ) {
    setCondition( condition );
  }


  /**
   * @return the serviceFields
   */
  public RowMetaInterface getServiceFields() {
    return serviceFields;
  }

  /**
   * @param serviceFields the serviceFields to set
   */
  public void setServiceFields( RowMetaInterface serviceFields ) {
    this.serviceFields = serviceFields;
  }

  /**
   * @return the condition
   */
  public Condition getCondition() {
    return condition;
  }

  /**
   * @param condition the condition to set
   */
  public void setCondition( Condition condition ) {
    this.condition = condition;
  }

  /**
   * @return the conditionClause
   */
  public String getConditionClause() {
    return conditionClause;
  }

  /**
   * @param conditionClause the conditionClause to set
   */
  public void setConditionClause( String conditionClause ) {
    this.conditionClause = conditionClause;
  }

  public boolean isEmpty() {
    return condition.isEmpty();
  }

  /**
   * @return the selectFields
   */
  public SQLFields getSelectFields() {
    return selectFields;
  }

  /**
   * @return the tableAlias
   */
  public String getTableAlias() {
    return tableAlias;
  }

  /**
   * Extract the list of having fields from this having condition
   *
   * @param aggFields
   * @param rowMeta
   * @return
   * @throws KettleSQLException
   */
  public List<SQLField> extractHavingFields( List<SQLField> selectFields, List<SQLField> aggFields,
      RowMetaInterface rowMeta ) throws KettleSQLException {
    List<SQLField> list = new ArrayList<SQLField>();

    // Get a list of all the lowest level field names and see if we can validate them as aggregation fields
    //
    List<String> expressions = new ArrayList<String>();
    addExpressions( condition, expressions );

    for ( String expression : expressions ) {
      // See if we already specified the aggregation in the Select clause, let's aggregate twice.
      //
      SQLField aggField = SQLField.searchSQLFieldByFieldOrAlias( aggFields, expression );
      if ( aggField == null ) {

        SQLField field = new SQLField( tableAlias, expression, serviceFields );
        if ( field.getAggregation() != null ) {
          field.setField( expression );
          list.add( field );
        }
      }
    }

    return list;
  }

  private void addExpressions( Condition condition, List<String> expressions ) {
    if ( condition.isAtomic() ) {
      if ( !expressions.contains( condition.getLeftValuename() ) ) {
        expressions.add( condition.getLeftValuename() );
      }
    } else {
      for ( Condition child : condition.getChildren() ) {
        addExpressions( child, expressions );
      }
    }
  }
}
