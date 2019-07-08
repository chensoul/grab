package com.javachen.grab.model.domain;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.javachen.grab.model.recom.Recommendation;
import lombok.Data;
import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.List;

@Data
@Document(collection = "MovieRecs")
public class MovieRecs {

    @Id
    @JsonIgnore
    private ObjectId _id;

    private Long mid;

    private List<Recommendation> recs;
}