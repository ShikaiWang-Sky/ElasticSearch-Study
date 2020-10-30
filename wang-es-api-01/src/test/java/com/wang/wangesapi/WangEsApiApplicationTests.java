package com.wang.wangesapi;

import com.alibaba.fastjson.JSON;
import com.wang.wangesapi.pojo.User;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.ArrayList;

@SpringBootTest
class WangEsApiApplicationTests {

    //Autowired 根据名称首字母转大写匹配类型, 这里和类型不一致,所以我们要用ID绑定
    @Autowired
    @Qualifier("restHighLevelClient")
    private RestHighLevelClient client;

    //测试索引的创建 所有的请求都使用Request创建
    @Test
    public void testCreateIndex() throws IOException {
        // 1. 创建索引请求
        CreateIndexRequest request = new CreateIndexRequest("wang_index");
        // 2. 执行创建请求 第二个参数我们一般使用默认的 RequestOptions.DEFAULT
        //indices ==> index的复数
        CreateIndexResponse createIndexResponse = client.indices().create(request, RequestOptions.DEFAULT);

        System.out.println(createIndexResponse);
    }

    // 测试获取索引
    @Test
    public void testExitIndex() throws IOException {
        GetIndexRequest request = new GetIndexRequest("wang_index");
        boolean exists = client.indices().exists(request, RequestOptions.DEFAULT);
        System.out.println(exists);
    }

    // 测试删除索引
    @Test
    public void testDeleteIndex() throws IOException {
        DeleteIndexRequest request = new DeleteIndexRequest("wang_index");
        AcknowledgedResponse delete = client.indices().delete(request, RequestOptions.DEFAULT);
        System.out.println(delete.isAcknowledged());
    }

    //测试添加文档
    @Test
    public void testAddDocument() throws IOException {
        //创建对象
        User user = new User("张三", 12);
        //创建请求
        IndexRequest request = new IndexRequest("wang_index");
        //创建规则 put/wang_index/_doc/1
        request.id("1");
        //设置过期规则, 与 request.timeout("1s") 效果一致
        request.timeout(TimeValue.timeValueSeconds(1));
        //将我们数据放入请求 (JSON)
        IndexRequest source = request.source(JSON.toJSONString(user), XContentType.JSON);
        //客户端发送请求
        IndexResponse response = client.index(request, RequestOptions.DEFAULT);

        System.out.println(response.toString());
        System.out.println(response.status());
    }

    //获取文档, 首先判断是否存在 get/index/_doc/1
    @Test
    public void testIsExist() throws IOException {
        GetRequest request = new GetRequest("wang_index", "1");
        //不获取我们返回的 _source 的上下文了, 效率更高
        request.fetchSourceContext(
                new FetchSourceContext(false)
        );
        request.storedFields("_none_");

        boolean exists = client.exists(request, RequestOptions.DEFAULT);
        System.out.println(exists);
    }

    //获取文档的信息
    @Test
    public void testGetDocument() throws IOException {
        GetRequest request = new GetRequest("wang_index", "1");
        GetResponse response = client.get(request, RequestOptions.DEFAULT);
        //打印文档的内容
        System.out.println(response.getSourceAsString());
        //返回的全内容和命令式是一样的
        System.out.println(response);
    }

    //更新文档信息
    @Test
    public void testUpdateDocument() throws IOException {
        UpdateRequest request = new UpdateRequest("wang_index", "1");
        request.timeout("1s");
        User user = new User("李四", 24);
        request.doc(JSON.toJSONString(user), XContentType.JSON);
        UpdateResponse response = client.update(request, RequestOptions.DEFAULT);
        System.out.println(response.status());
    }

    //删除文档记录
    @Test
    public void testDeleteDocument() throws IOException {
        DeleteRequest request = new DeleteRequest("wang_index", "1");
        request.timeout("1s");

        DeleteResponse response = client.delete(request, RequestOptions.DEFAULT);
        System.out.println(response.status());
    }

    //批量导入数据
    @Test
    void testBulkRequest() throws IOException {
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.timeout(TimeValue.timeValueSeconds(10));

        ArrayList<User> userList = new ArrayList<>();
        userList.add(new User("张三1号", 3));
        userList.add(new User("张三2号", 3));
        userList.add(new User("张三3号", 3));
        userList.add(new User("李四1号", 3));
        userList.add(new User("李四2号", 3));
        userList.add(new User("李四3号", 3));

        //批处理请求
        for (int i = 0; i < userList.size(); i++) {
            bulkRequest.add(
                    new IndexRequest("wang_index")
                            .id("" + i)
                            .source(JSON.toJSONString(userList.get(i)), XContentType.JSON));

        }

        BulkResponse responses = client.bulk(bulkRequest, RequestOptions.DEFAULT);
        //是否存在失败的数据 ==> 返回false则说明全部插入成功!
        System.out.println(responses.hasFailures());
    }

    //查询
    //SearchRequest 搜索请求
    //SearchSourceBuilder 条件构造
    //HighlightBuilder 构建高亮
    //TermQueryBuilder 精确查询
    //XXXXQueryBuilder 对应我们看到的所有功能
    @Test
    public void testSearch() throws IOException {
        SearchRequest searchRequest = new SearchRequest("wang_index");
        //构建搜索的条件
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        //查询条件, 可以使用 QueryBuilders 工具来实现
        //QueryBuilders.termQuery 精确匹配
        //QueryBuilder.matchAllQuery 匹配所有
        MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("name", "张三1号");
        sourceBuilder.query(matchQueryBuilder)
                .timeout(TimeValue.timeValueSeconds(1))
                //分页
                .from(0).size(3);
        searchRequest.source(sourceBuilder);

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        System.out.println(JSON.toJSONString(searchResponse.getHits()));
        System.out.println("=========================================");
        //遍历搜索结果, 取出其中的documentFields.getSourceAsMap, 就可以得到JSON MAP的结果
        for (SearchHit documentFields : searchResponse.getHits()) {
            System.out.println(documentFields.getSourceAsMap());
        }

    }

}
