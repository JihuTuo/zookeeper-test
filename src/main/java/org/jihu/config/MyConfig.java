package org.jihu.config;

import lombok.*;

@Data
@ToString
@NoArgsConstructor
@Builder
@AllArgsConstructor
public class MyConfig {
    private String key;
    private String name;
}
