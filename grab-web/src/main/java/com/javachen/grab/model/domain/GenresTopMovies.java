package com.javachen.grab.model.domain;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.javachen.grab.model.recom.Recommendation;
import lombok.Data;
import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.List;

/**
 * @author june
 * @createTime 2019-07-08 13:36
 * @see
 * @since
 */
@Data
@Document(collection = "GenresTopMovies")
public class GenresTopMovies {

    @Id
    @JsonIgnore
    private ObjectId _id;

    private String genres;

    private List<Recommendation> recs;
}
