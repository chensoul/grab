package com.javachen.grab.model.domain;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Data;
import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.ArrayList;
import java.util.List;

@Document(collection = "User")
@Data
public class User {

    @Id
    @JsonIgnore
    private ObjectId _id;

    private Long uid;

    private String username;

    private String password;

    private boolean first;

    private long timestamp;

    private List<String> prefGenres = new ArrayList<>();

    public void setUsername(String username) {
        this.uid = Long.valueOf(username.hashCode());
        this.username = username;
    }

    public boolean passwordMatch(String password) {
        return this.password.compareTo(password) == 0;
    }
}