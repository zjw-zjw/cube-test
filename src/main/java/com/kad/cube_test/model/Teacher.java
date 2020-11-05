package com.kad.cube_test.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.sql.Date;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Teacher {

    // 姓名
    private String name;
    // 工作城市
    private String city;
    // 工作时间
    private Date work_time;

}
