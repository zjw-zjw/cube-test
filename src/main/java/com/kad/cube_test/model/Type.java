package com.kad.cube_test.model;

import lombok.*;

import java.math.BigDecimal;

@Data
public class Type {
    private String _varchar;
    private int _int;
    private long bigint;
    private short smallint;
    private Byte tinyint;
    private BigDecimal decimal_5_4;
    private BigDecimal decimal_18_4;
    protected Double _double;
    private Float _float;

    public Type(String varchar, Integer _int, Long bigint, Short smallint, Byte tinyint, BigDecimal decimal_5_4, BigDecimal decimal_18_4, Double _double, Float _float) {
        this._varchar = varchar;
        this._int = _int;
        this.bigint = bigint;
        this.smallint = smallint;
        this.tinyint = tinyint;
        this.decimal_5_4 = decimal_5_4;
        this.decimal_18_4 = decimal_18_4;
        this._double = _double;
        this._float = _float;
    }

    public Type() {}

    public String getVarchar() {
        return _varchar;
    }

    public void setVarchar(String varchar) {
        this._varchar = varchar;
    }

    public Integer get_int() {
        return _int;
    }

    public void set_int(Integer _int) {
        this._int = _int;
    }

    public Long getBigint() {
        return bigint;
    }

    public void setBigint(Long bigint) {
        this.bigint = bigint;
    }

    public Short getSmallint() {
        return smallint;
    }

    public void setSmallint(Short smallint) {
        this.smallint = smallint;
    }

    public Byte getTinyint() {
        return tinyint;
    }

    public void setTinyint(Byte tinyint) {
        this.tinyint = tinyint;
    }

    public BigDecimal getDecimal_5_4() {
        return decimal_5_4;
    }

    public void setDecimal_5_4(BigDecimal decimal_5_4) {
        this.decimal_5_4 = decimal_5_4;
    }

    public BigDecimal getDecimal_18_4() {
        return decimal_18_4;
    }

    public void setDecimal_18_4(BigDecimal decimal_18_4) {
        this.decimal_18_4 = decimal_18_4;
    }

    public Double get_double() {
        return _double;
    }

    public void set_double(Double _double) {
        this._double = _double;
    }

    public Float get_float() {
        return _float;
    }

    public void set_float(Float _float) {
        this._float = _float;
    }


}
