package com.wang.wangesjd.service;

import com.alibaba.fastjson.JSON;
import com.wang.wangesjd.pojo.Content;
import com.wang.wangesjd.utils.HtmlParseUtil;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Service
public class ContentService {

    @Autowired
    @Qualifier("restHighLevelClient")
    private RestHighLevelClient rest;

    @Autowired
    private HtmlParseUtil htmlParseUtil;


    //解析数据, 放入ES索引中
    public Boolean parseContent(String keywords) throws IOException {
        List<Content> contents = htmlParseUtil.parseJD(keywords);
        //把查询的数据放入ES中
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.timeout(TimeValue.timeValueMinutes(2L));

        for (int i = 0; i < contents.size(); i++) {
            bulkRequest.add(new IndexRequest("jd_goods")
                    .source(JSON.toJSONString(contents.get(i)), XContentType.JSON));
        }

        BulkResponse bulk = rest.bulk(bulkRequest, RequestOptions.DEFAULT);
        return !bulk.hasFailures();
    }

    //获取这些数据(从ES索引中), 实现搜索功能
//    public List<Map<String, Object>> searchPage(String keyword, int pageNo, int pageSize) throws IOException {
//        if (pageNo <= 1) {
//            pageNo = 1;
//        }
//        //条件搜索
//        SearchRequest searchRequest = new SearchRequest("jd_goods");
//        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
//        //精准匹配
//        MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("title", keyword);
//        sourceBuilder.query(matchQueryBuilder)
//                .timeout(TimeValue.timeValueMinutes(1L));
//        //分页
//        sourceBuilder.from(pageNo)
//                .size(pageSize);
//        //执行搜索
//        searchRequest.source(sourceBuilder);
//        SearchResponse searchResponse = rest.search(searchRequest, RequestOptions.DEFAULT);
//        //解析结果
//        List<Map<String, Object>> list = new ArrayList<>();
//        for (SearchHit documentFields : searchResponse.getHits()) {
//            list.add(documentFields.getSourceAsMap());
//        }
//        return list;
//    }

    //实现搜索高亮
    public List<Map<String, Object>> searchPage(String keyword, int pageNo, int pageSize) throws IOException {
        if (pageNo <= 1) {
            pageNo = 1;
        }
        //条件搜索
        SearchRequest searchRequest = new SearchRequest("jd_goods");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        //精准匹配
        MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("title", keyword);
        sourceBuilder.query(matchQueryBuilder)
                .timeout(TimeValue.timeValueMinutes(1L));
        //高亮
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        //定义要高亮的标签和样式
        highlightBuilder.field("title")
                .preTags("<span style='color:red'>")
                .postTags("</span>")
                .requireFieldMatch(false);          //是否需要高亮多个字段
        sourceBuilder.highlighter(highlightBuilder);
        //分页
        sourceBuilder.from(pageNo)
                .size(pageSize);
        //执行搜索
        searchRequest.source(sourceBuilder);
        SearchResponse searchResponse = rest.search(searchRequest, RequestOptions.DEFAULT);
        //解析结果
        List<Map<String, Object>> list = new ArrayList<>();
        for (SearchHit documentFields : searchResponse.getHits()) {

            //解析高亮的字段
            Map<String, HighlightField> highlightFields = documentFields.getHighlightFields();
            HighlightField title = highlightFields.get("title");
            Map<String, Object> sourceAsMap = documentFields.getSourceAsMap();  //这里是原来的结果(不含高亮)

            if(title != null) {
                Text[] fragments = title.fragments();
                String highlightTitle = "";
                for (Text text : fragments) {
                    highlightTitle += text;
                }
                //将高亮字段替换没有高亮的字段
                sourceAsMap.put("title", highlightTitle);
            }

            list.add(sourceAsMap);

        }
        return list;
    }


}
