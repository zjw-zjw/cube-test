package com.kad.cube_test.model;

import lombok.*;


@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
@Setter
@Getter
public class Student {
    public int id;
    public String name;
    public String password;
    public int age;
}
