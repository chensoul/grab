package com.javachen.grab.model.domain;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Data;
import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.Date;

@Data
@Document(collection = "Rating")
public class Rating {

    @Id
    @JsonIgnore
    private ObjectId _id;

    private Long uid;

    private Long mid;

    private double score;

    private Long timestamp;

    public Rating() {
    }

    public Rating(Long uid, Long mid, double score) {
        this.uid = uid;
        this.mid = mid;
        this.score = score;
        this.timestamp = new Date().getTime();
    }


}