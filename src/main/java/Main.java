import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Created by xjk on 17-5-8.
 */
public class Main {
    //定义index
    private static final String index = "store";
    //定义type
    private static final String type = "book";

    public static void main(String[] args) {
        Client client = null;
        try {
            client = new PreBuiltTransportClient(Settings.EMPTY)
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
            //创建索引，有则先删除
            recreateIndex(client);
            //插入数据
            doIndex(client);
            //搜索全部数据
            searchAll(client);
            //搜索部分数据
            searchRange(client);
            //根据关键字搜索
            searchKeyWord(client);
            //高亮关键字
            searchHightlight(client);
            //排序
            searchOrdered(client);
            //根据id搜索
            findById(client);
            //更新
            updateById(client);
            //删除
            deleteById(client);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }



    /**
     * 创建索引，有则先删除
     * @param client
     */
    private static void recreateIndex(Client client) {
        if (client.admin().indices().prepareExists(index).execute().actionGet()
                .isExists()) {
            DeleteIndexResponse deleteIndexResponse = client.admin().indices()
                    .delete(new DeleteIndexRequest(index)).actionGet();
            System.out.println("delete index :");
            System.out.println(deleteIndexResponse);
        }

        CreateIndexResponse createIndexResponse = client.admin().indices()
                .prepareCreate(index).execute().actionGet();
        System.out.println("create index :");
        System.out.println(createIndexResponse);
    }


    /**
     * 插入数据
     * @param client
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    private static void doIndex(final Client client) {

        Map s11 = new LinkedHashMap();
        s11.put("title", "Think in java");
        s11.put("origin", "美国");
        s11.put("description", "初级java开发人员必读的书");
        s11.put("author", "Bruce Eckel");
        s11.put("price", 108);

        Map s12 = new LinkedHashMap();
        s12.put("title", "Head First Java");
        s12.put("origin", "英国");
        s12.put("description", "java入门教材");
        s12.put("author", "Kathy Sierra");
        s12.put("price", 54);

        Map s21 = new LinkedHashMap();
        s21.put("title", "Design Pattern");
        s21.put("origin", "法国");
        s21.put("description", "程序员不得不读的设计模式");
        s21.put("author", "Kathy Sierra");
        s21.put("price", 89);

        Map s22 = new LinkedHashMap();
        s22.put("title", "黑客与画家");
        s22.put("origin", "法国");
        s22.put("description", "读完之后脑洞大开");
        s22.put("author", "Paul Graham");
        s22.put("price", 35);

        BulkResponse bulkResponse = client
                .prepareBulk()
                .add(client.prepareIndex(index, type).setId("11").setSource(s11).setOpType(IndexRequest.OpType.INDEX).request())
                .add(client.prepareIndex(index, type).setId("12").setSource(s12).setOpType(IndexRequest.OpType.INDEX).request())
                .add(client.prepareIndex(index, type).setId("21").setSource(s21).setOpType(IndexRequest.OpType.INDEX).request())
                .add(client.prepareIndex(index, type).setId("22").setSource(s22).setOpType(IndexRequest.OpType.INDEX).request())
                .execute().actionGet();
        if (bulkResponse.hasFailures()) {
            System.err.println("index docs ERROR:" + bulkResponse.buildFailureMessage());
        } else {
            System.out.println("index docs SUCCESS:");
            System.out.println(bulkResponse);
        }
    }

    /**
     * 查询所有
     */
    private static void searchAll(Client client) {
        SearchResponse response = client.prepareSearch(index)
                .setQuery(QueryBuilders.matchAllQuery())
                .setExplain(true).execute().actionGet();
        System.out.println("searchAll : ");
        for (SearchHit searchHit : response.getHits()) {
            System.out.println("********");
            System.out.println(searchHit.getSource());
        }
    }

    /**
     * 关键词查询
     *
     * @param client
     */
    private static void searchKeyWord(Client client) {
        SearchResponse response = client.prepareSearch(index)
                //查询所有字段匹配关键字
                .setQuery(QueryBuilders.matchQuery("_all", "法国"))
                //设置最小匹配程度
//                .setQuery(QueryBuilders.matchQuery("_all", "法国").minimumShouldMatch("100%"))
                .execute().actionGet();
        System.out.println("searchKeyWord : ");
        System.out.println(response);
    }

    /**
     * 数值范围过滤
     *
     * @param client
     */
    private static void searchRange(Client client) {
        SearchResponse response = client.prepareSearch(index).
                //大于80，小于100
                setQuery(QueryBuilders.rangeQuery("price").gt(80).lt(100))
                .execute()
                .actionGet();
        System.out.println("searchRange : ");
        System.out.println(response);
    }

    /**
     * 排序
     *
     * @param client
     */
    private static void searchOrdered(Client client) {
        SearchResponse response = client.prepareSearch(index)
                .setQuery(QueryBuilders.matchAllQuery())
                //根据价格降序排序
                .addSort(SortBuilders.fieldSort("price")
                        .order(SortOrder.DESC)).execute().actionGet();
        System.out.println("searchOrdered : ");
        System.out.println(response);
    }

    /**
     * 高亮关键字
     * @param client
     */
    private static void searchHightlight(Client client) {
        //高亮多个字段
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.field("title");
        highlightBuilder.field("description");
        SearchResponse response = client.prepareSearch(index)
                //单条件匹配，高亮时只能高亮该字段
//                .setQuery(QueryBuilders.matchQuery("title", "java"))
                //多条件匹配，高亮时只能高亮多个字段
                .setQuery(QueryBuilders.multiMatchQuery("开发人员必读", "title", "description"))
                .highlighter(highlightBuilder)
                .execute()
                .actionGet();
        System.out.println("searchHightlight : ");
        System.out.println(response);
    }

    /**
     * 根据id查找
     * @param client
     */
    private static void findById(final Client client) {
        String id="12";
        GetResponse response = client.prepareGet(index, type, id).get();
        System.out.println("findById");
        System.out.println(response);
    }

    /**
     * 删除
     * @param client
     */
    private static void deleteById(Client client) {
        String id="12";
        DeleteResponse response = client.prepareDelete(index, type, id).get();
    }

    /**
     * 更新
     * @param client
     */
    private static void updateById(Client client) {
        try {
            String id="11";
            client.prepareUpdate(index, type, id)
                    .setDoc(jsonBuilder()
                            .startObject()
                            .field("title", "白鹿原")
                            .endObject())
                    .get();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
