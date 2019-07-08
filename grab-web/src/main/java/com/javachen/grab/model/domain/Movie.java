package com.javachen.grab.model.domain;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Data;
import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

@Data
@Document(collection = "Movie")
public class Movie {

    @Id
    @JsonIgnore
    private ObjectId _id;

    private Long mid;

    private String name;

    private String descri;

    private String timelong;

    private String issue;

    private String shoot;

    private Double score;

    private String language;

    private String genres;

    private String actors;

    private String directors;


}