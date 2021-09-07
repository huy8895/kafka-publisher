package com.example.kafkapublisher;

import lombok.*;

@EqualsAndHashCode(callSuper = true)
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class User extends AbstractClass{
    private int id;
    private String name;
    private String[] address;
}
