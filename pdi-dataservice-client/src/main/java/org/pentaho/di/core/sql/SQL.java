package org.pentaho.di.core.sql;

import org.apache.calcite.sql.fun.SqlCase;
import org.pentaho.di.core.row.RowMetaInterface;

import java.util.Collections;
import java.util.Optional;

public class SQL {

  private RowMetaInterface rowMeta;
  private SQLFields selectFields;
  private SQLCondition whereCondition;
  private SQLFields groupFields;
  private SQLCondition havingCondition;
  private SQLFields orderFields;
  private SQLLimit limitValues;
  private String serviceName;
  private String sqlString;

  private SqlCase sqlCase;

  public SQL( String sql ) {
    sqlString = sql;
  }

  public SQL() {
  }

  public static Builder builder() {
    return new Builder();
  }

  public RowMetaInterface getRowMeta() {
    return rowMeta;
  }

  public SQLLimit getLimitValues() {
    return limitValues;
  }

  public SQLFields getOrderFields() {
    return nullToEmpty( orderFields );
  }

  public SQLCondition getHavingCondition() {
    return havingCondition;
  }

  public SQLFields getGroupFields() {
    return nullToEmpty( groupFields );
  }

  private SQLFields nullToEmpty( SQLFields fields ) {
    return Optional.ofNullable( fields )
      .orElse( new SQLFields( rowMeta, Collections.emptyList() ) );
  }

  public SQLCondition getWhereCondition() {
    return whereCondition;
  }

  public SQLFields getSelectFields() {
    return nullToEmpty( selectFields );
  }

  public String getServiceName() {
    return serviceName;
  }

  public String getSqlString() {
    return sqlString;
  }

  public static final class Builder {
    private SQLFields groupFields;
    private RowMetaInterface rowMeta;
    private SQLFields selectFields;
    private SQLCondition whereCondition;
    private SQLCondition havingCondition;
    private SQLFields orderFields;
    private SQLLimit limitValues;
    private String serviceName;
    private String sqlString;

    private Builder() {
    }

    public Builder withGroupFields( SQLFields groupFields ) {
      this.groupFields = groupFields;
      return this;
    }

    public Builder withRowMeta( RowMetaInterface rowMeta ) {
      this.rowMeta = rowMeta;
      return this;
    }

    public Builder withSelectFields( SQLFields selectFields ) {
      this.selectFields = selectFields;
      return this;
    }

    public Builder withWhereCondition( SQLCondition whereCondition ) {
      this.whereCondition = whereCondition;
      return this;
    }

    public Builder withHavingCondition( SQLCondition havingCondition ) {
      this.havingCondition = havingCondition;
      return this;
    }

    public Builder withOrderFields( SQLFields orderFields ) {
      this.orderFields = orderFields;
      return this;
    }

    public Builder withLimitValues( SQLLimit limitValues ) {
      this.limitValues = limitValues;
      return this;
    }

    public Builder withServiceName( String serviceName ) {
      this.serviceName = serviceName;
      return this;
    }

    public Builder withSqlString( String sqlString ) {
      this.sqlString = sqlString;
      return this;
    }

    public SQL build() {
      SQL dSQuery = new SQL();
      dSQuery.havingCondition = this.havingCondition;
      dSQuery.groupFields = this.groupFields;
      dSQuery.orderFields = this.orderFields;
      dSQuery.selectFields = this.selectFields;
      dSQuery.whereCondition = this.whereCondition;
      dSQuery.limitValues = this.limitValues;
      dSQuery.rowMeta = this.rowMeta;
      dSQuery.serviceName = serviceName;
      dSQuery.sqlString = sqlString;
      return dSQuery;
    }
  }
}
