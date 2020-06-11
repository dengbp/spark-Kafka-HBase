package com.yr.router.strategy;

/**
 * @author dengbp
 * @ClassName StrategyType
 * @Description TODO
 * @date 2020-03-12 11:47
 */
public enum StrategyType {

    HBASESTRATEGY("01","hbase"),
    MYSQLSTRATEGY("02","mysql");

    StrategyType(String code, String des) {
        this.code = code;
        this.name = des;
    }

    private final String code;
    private final String name;

    public static StrategyType getByCode(String code){
        if(code == null){
            return null;
        }
        for(StrategyType e : StrategyType.values()){
            if(e.getCode().equals(code)){
                return e;
            }
        }
        return null;
    }

    public String getCode() {
        return code;
    }

    public String getDes() {
        return name;
    }
}
