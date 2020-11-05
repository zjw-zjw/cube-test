package com.kad.cube_test.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NoArgsConstructor;

@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PlayerData {
    // 赛季
    public String season;
    // 球员
    public String player;
    // 出场
    public String play_num;
    // 首发
    public Integer first_court;
    // 时间
    public Double time;
    // 助攻
    public Double assists;
    // 抢断
    public Double steals;
    // 盖帽
    public Double blocks;
    // 得分
    public Double scores;
}
