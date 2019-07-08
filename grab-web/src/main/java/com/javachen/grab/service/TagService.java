package com.javachen.grab.service;

import com.javachen.grab.model.domain.Tag;
import com.javachen.grab.repository.TagRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class TagService {

    @Autowired
    private TagRepository tagRepository;


    public void newTag(Tag tag){
        tagRepository.save(tag);
    }

    private void updateElasticSearchIndex(Tag tag){
//        GetResponse getResponse = esClient.prepareGet(Constant.ES_INDEX,Constant.ES_MOVIE_TYPE,String.valueOf(tag.getMid())).get();
//        Object value = getResponse.getSourceAsMap().get("tags");
//        UpdateRequest updateRequest = new UpdateRequest(Constant.ES_INDEX,Constant.ES_MOVIE_TYPE,String.valueOf(tag.getMid()));
//        try{
//            if(value == null){
//                updateRequest.doc(XContentFactory.jsonBuilder().startObject().field("tags",tag.getTag()).endObject());
//            }else{
//                updateRequest.doc(XContentFactory.jsonBuilder().startObject().field("tags",value+"|"+tag.getTag()).endObject());
//            }
//            esClient.update(updateRequest).get();
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        } catch (ExecutionException e) {
//            e.printStackTrace();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
    }

    public List<Tag> findAllByMid(Long mid){
       return tagRepository.findAllByMid(mid);
    }

    public List<Tag> findAllByUidAndMid(Long uid, Long mid){
        return tagRepository.findAllByUidAndMid(uid,mid);
    }

}
