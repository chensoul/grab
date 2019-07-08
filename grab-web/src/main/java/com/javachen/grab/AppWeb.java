package com.javachen.grab;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class AppWeb {


    public static void main(String[] args)  {
        System.setProperty("es.set.netty.runtime.available.processors", "false");

        SpringApplication.run(AppWeb.class,args);

//        ApplicationContext context = new ClassPathXmlApplicationContext("classpath:application.xml");
//
//        Settings settings = Settings.builder().put("cluster.name","es-cluster").build();
//        TransportClient esClient = new PreBuiltTransportClient(settings);
//        esClient.addTransportAddress(new TransportAddress(InetAddress.getByName("linux"), 9300));
//
//        GetResponse getResponse = esClient.prepareGet(Constant.ES_INDEX,Constant.ES_MOVIE_TYPE,"3062").get();
//
//        //Map<String,GetField> filed = getResponse.getFields();
//
//        Object value = getResponse.getSourceAsMap().get("tags");
//
//        if(value == null){
//            UpdateRequest updateRequest = new UpdateRequest(Constant.ES_INDEX,Constant.ES_MOVIE_TYPE,"3062");
//            updateRequest.doc(XContentFactory.jsonBuilder().startObject()
//            .field("tags","abc")
//            .endObject());
//            esClient.update(updateRequest).get();
//        }else{
//            UpdateRequest updateRequest = new UpdateRequest(Constant.ES_INDEX,Constant.ES_MOVIE_TYPE,"2542");
//            updateRequest.doc(XContentFactory.jsonBuilder().startObject()
//                    .field("tags",value+"|abc")
//            .endObject());
//            esClient.update(updateRequest).get();
//        }
//
//        System.out.println(Math.round(4.466D));

    }

}
