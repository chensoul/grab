package com.javachen.grab.model.recom;

import lombok.Data;

import java.util.Comparator;

/**
 * 推荐项目的包装
 */
@Data
public class Recommendation implements Comparator<Recommendation> {

    // 电影ID
    private Long mid;

    // 电影的推荐得分
    private Double score;

    public Recommendation() {
    }

    public Recommendation(Long mid, Double score) {
        this.mid = mid;
        this.score = score;
    }


    @Override
    public int compare(Recommendation o1, Recommendation o2) {
        return o1.getScore() > o2.getScore() ? -1 : 1;
    }
}
