package com.javachen.grab.model.domain;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Data;
import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.io.Serializable;
import java.util.Date;
import java.util.UUID;

@Data
@Document(collection = "Tag")
public class Tag {

    @Id
    @JsonIgnore
    private ObjectId _id;

    private Long uid;

    private Long mid;

    private String tag;

    private long timestamp;

    public Tag(Long uid, Long mid, String tag) {
        this.uid = uid;
        this.mid = mid;
        this.tag = tag;
        this.timestamp = new Date().getTime();
    }

    public Tag() {
    }


}