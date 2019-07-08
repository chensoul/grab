package com.javachen.grab.service;

import com.javachen.grab.model.domain.Rating;
import com.javachen.grab.repository.RatingRepository;
import com.javachen.grab.utils.Constant;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class RatingService {

    @Autowired
    private RatingRepository ratingRepository;
    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    private void updateRedis(Rating rating) {
        if (stringRedisTemplate.hasKey("uid:" + rating.getUid()) && stringRedisTemplate.opsForSet().size("uid:" + rating.getUid()) >= Constant.REDIS_MOVIE_RATING_QUEUE_SIZE) {
            stringRedisTemplate.opsForList().rightPop("uid:" + rating.getUid());
        }
        stringRedisTemplate.opsForList().leftPush("uid:" + rating.getUid(), rating.getMid() + ":" + rating.getScore());
    }

    public void newRating(Rating rating) {
        updateRedis(rating);
        ratingRepository.save(rating);
    }

    public boolean ratingExist(Long uid, Long mid)  {
        return null != findByUidAndMid(uid, mid);
    }

    public void updateRating(Rating rating) {
        ratingRepository.save(rating);
    }


    public Rating findByUidAndMid(Long uid, Long mid) {
        return ratingRepository.findByUidAndMid(uid,mid);
    }

    public List<Rating> findAllByUid(Long uid) {
        return ratingRepository.findAllByUid(uid);
    }

    public void removeRating(Long uid, Long mid){
        Rating rating=findByUidAndMid(uid,mid);
        ratingRepository.delete(rating);
    }

    public int[] getMyRatingStat(Long uid) {
        List<Rating> ratings=ratingRepository.findAllByUid(uid);
        int[] stats = new int[10];
        for (Rating rating : ratings) {
            Long index = Math.round(rating.getScore() / 0.5);
            stats[index.intValue()] = stats[index.intValue()] + 1;
        }
        return stats;
    }

}
