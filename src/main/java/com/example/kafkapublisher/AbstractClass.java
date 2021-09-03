package com.example.kafkapublisher;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.*;

@Getter
@Setter
@NoArgsConstructor
public class AbstractClass {

    @SneakyThrows
    @Override
    public String toString() {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(this);
    }
}
