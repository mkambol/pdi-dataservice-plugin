/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2013 by Pentaho : http://www.pentaho.com
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

import com.google.common.collect.Lists;
import org.pentaho.di.core.exception.KettleSQLException;
import org.pentaho.di.core.row.RowMetaInterface;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class SQLFields {
  private String tableAlias;
  private RowMetaInterface serviceFields;
  private String fieldsClause;
  private List<SQLField> fields;
  private SQLFields selectFields;

  private boolean distinct;

  public SQLFields( String tableAlias, RowMetaInterface serviceFields, String fieldsClause ) throws KettleSQLException {
    this( tableAlias, serviceFields, fieldsClause, false );
  }

  public SQLFields( String tableAlias, RowMetaInterface serviceFields, String fieldsClause, boolean orderClause ) throws KettleSQLException {
    this( tableAlias, serviceFields, fieldsClause, orderClause, null );
  }

  public SQLFields( String tableAlias, RowMetaInterface serviceFields, String fieldsClause, boolean orderClause,
    SQLFields selectFields ) throws KettleSQLException {
    this.tableAlias = tableAlias;
    this.serviceFields = serviceFields;
    this.fieldsClause = fieldsClause;
    this.selectFields = selectFields;
    fields = Lists.newArrayList();

    distinct = false;

  }

  public SQLFields( RowMetaInterface rowMeta, List<SQLField> fields ) {
    this.fields = fields;
    this.serviceFields = rowMeta;
  }



  public List<SQLField> getFields() {
    return Optional.ofNullable( fields )
      .orElse( Collections.emptyList() );
  }

  public List<SQLField> getNonAggregateFields() {
    List<SQLField> list = new ArrayList<SQLField>();
    for ( SQLField field : fields ) {
      if ( field.getAggregation() == null ) {
        list.add( field );
      }
    }
    return list;
  }

  public List<SQLField> getAggregateFields() {
    List<SQLField> list = new ArrayList<SQLField>();
    for ( SQLField field : fields ) {
      if ( field.getAggregation() != null ) {
        list.add( field );
      }
    }
    return list;
  }

  public boolean isEmpty() {
    return fields.isEmpty();
  }

  /**
   * Find a field by it's field name (not alias)
   *
   * @param fieldName
   *          the name of the field
   * @return the field or null if nothing was found.
   */
  public SQLField findByName( String fieldName ) {
    for ( SQLField field : fields ) {
      if ( field.getField().equalsIgnoreCase( fieldName ) ) {
        return field;
      }
    }
    return null;
  }

  /**
   * @return the serviceFields
   */
  public RowMetaInterface getServiceFields() {
    return serviceFields;
  }

  /**
   * @param serviceFields
   *          the serviceFields to set
   */
  public void setServiceFields( RowMetaInterface serviceFields ) {
    this.serviceFields = serviceFields;
  }

  /**
   * @return the fieldsClause
   */
  public String getFieldsClause() {
    return fieldsClause;
  }

  /**
   * @param fieldsClause
   *          the fieldsClause to set
   */
  public void setFieldsClause( String fieldsClause ) {
    this.fieldsClause = fieldsClause;
  }

  /**
   * @return the selectFields
   */
  public SQLFields getSelectFields() {
    return selectFields;
  }

  /**
   * @param selectFields
   *          the selectFields to set
   */
  public void setSelectFields( SQLFields selectFields ) {
    this.selectFields = selectFields;
  }

  /**
   * @param fields
   *          the fields to set
   */
  public void setFields( List<SQLField> fields ) {
    this.fields = fields;
    indexFields();
  }

  private void indexFields() {
    for ( int i = 0; i < fields.size(); i++ ) {
      fields.get( i ).setFieldIndex( i );
    }
  }

  /**
   * @return true if one or more fields is an aggregation.
   */
  public boolean hasAggregates() {
    for ( SQLField field : fields ) {
      if ( field.getAggregation() != null ) {
        return true;
      }
    }
    return false;
  }

  public List<SQLField> getIifFunctionFields() {
    List<SQLField> list = new ArrayList<SQLField>();

    for ( SQLField field : fields ) {
      if ( field.getIif() != null ) {
        list.add( field );
      }
    }

    return list;
  }

  public List<SQLField> getRegularFields() {
    List<SQLField> list = new ArrayList<SQLField>();

    for ( SQLField field : fields ) {
      if ( field.getIif() == null && field.getAggregation() == null && field.getValueData() == null ) {
        list.add( field );
      }
    }

    return list;
  }

  public List<SQLField> getConstantFields() {
    List<SQLField> list = new ArrayList<SQLField>();

    for ( SQLField field : fields ) {
      if ( field.getValueMeta() != null && field.getValueData() != null ) {
        list.add( field );
      }
    }

    return list;
  }

  /**
   * @return the distinct
   */
  public boolean isDistinct() {
    return distinct;
  }

  /**
   * @return the tableAlias
   */
  public String getTableAlias() {
    return tableAlias;
  }

}
