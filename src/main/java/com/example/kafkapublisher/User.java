package com.example.kafkapublisher;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.*;

import java.util.Arrays;

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
