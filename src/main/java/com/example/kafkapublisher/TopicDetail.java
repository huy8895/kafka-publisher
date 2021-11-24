package com.example.kafkapublisher;

import lombok.Builder;
import lombok.Data;

import java.util.List;

@Data
@Builder
public class TopicDetail {
    private String name;
    private int partitions;
    private List<PartitionInfo> partitionInfo;
}

@Data
@Builder
class PartitionInfo {
    private int partition;
    private Node leader;

}

@Data
@Builder
class Node {
    private int id;
    private String idString;
    private String host;
    private int port;
    private String rack;
}
