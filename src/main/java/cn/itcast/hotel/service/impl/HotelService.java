package cn.itcast.hotel.service.impl;

import cn.itcast.hotel.mapper.HotelMapper;
import cn.itcast.hotel.pojo.Hotel;
import cn.itcast.hotel.pojo.HotelDoc;
import cn.itcast.hotel.pojo.PageResult;
import cn.itcast.hotel.pojo.RequestParams;
import cn.itcast.hotel.service.IHotelService;
import com.alibaba.fastjson.JSON;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.SuggestBuilders;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class HotelService extends ServiceImpl<HotelMapper, Hotel> implements IHotelService {

    @Autowired
    private RestHighLevelClient client;

    @Autowired
    private IHotelService hotelService;

    @Override
    public PageResult search(RequestParams params){

        try {
            //1.准备request
            SearchRequest request = new SearchRequest("hotel");

            //2.准备DSL
            //2.1搜索
            buildBasicQuery(params,request);

            //2.2分页
            int page = params.getPage();
            int size = params.getSize();
            request.source().from((page-1)*size).size(size);

            //2.3排序
            String location = params.getLocation();
            if(location!=null&&!location.equals("")){
                request.source().sort(SortBuilders.geoDistanceSort("location",
                        new GeoPoint(location)).
                        order(SortOrder.ASC)
                        .unit(DistanceUnit.KILOMETERS)
                );
            }

            //3.发送请求
            SearchResponse response = client.search(request, RequestOptions.DEFAULT);

            //4.解析响应
            return handleResponse(response);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Map<String, List<String>> getFilters(RequestParams params) {

        try {
            //1.准备request
            SearchRequest request = new SearchRequest("hotel");

            //2.准备DSL
            //准备Query
            buildBasicQuery(params,request);

            request.source().size(0);
            //聚合
            buildAggregation(request);

            //3.发送请求
            SearchResponse response = client.search(request, RequestOptions.DEFAULT);

            //4.解析结果
            Map<String, List<String>> result = new HashMap<>();
            Aggregations aggregations = response.getAggregations();

            List<String> brandList = getAggByName(aggregations, "brandAgg");
            result.put("品牌",brandList);
            List<String> cityList = getAggByName(aggregations, "cityAgg");
            result.put("城市",cityList);
            List<String> starList = getAggByName(aggregations, "starAgg");
            result.put("星级",starList);

            return result;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }


    }



    /**
     * 对品牌 城市 星级字段聚合
     * @param request
     */

    private void buildAggregation(SearchRequest request) {
        request.source().aggregation(AggregationBuilders.terms("brandAgg")
        .field("brand").size(100));
        request.source().aggregation(AggregationBuilders.terms("cityAgg")
        .field("city").size(100));
        request.source().aggregation(AggregationBuilders.terms("starAgg")
        .field("starName").size(100));

    }

    private PageResult handleResponse(SearchResponse response) {
        //解析响应
        SearchHits searchHits = response.getHits();

        //4.1获取总条数
        long total = searchHits.getTotalHits().value;
        System.out.println("共搜索到"+total+"条数据");
        //4.2文档数组
        SearchHit[] hits = searchHits.getHits();
        List<HotelDoc> hotels = new ArrayList<>();
        //4.3遍历
        for (SearchHit hit:hits){
            //获取文档source
            String json = hit.getSourceAsString();
            //反序列化
            HotelDoc hotelDoc = JSON.parseObject(json, HotelDoc.class);

            //获取排序值
            Object[] sortValues = hit.getSortValues();
            if(sortValues.length>0){
                Object sortValue = sortValues[0];
                hotelDoc.setDistance(sortValue);
            }

            hotels.add(hotelDoc);
        }

        return new PageResult(total,hotels);
    }

    /**
     * 构建思想 加上条件过滤 采用布尔查询 关键字搜索放在must中 参与算分
     * 而过滤条件放入filter中 不参与算分 把Query赋在request中
     * @param params
     * @param request
     */
    private void buildBasicQuery(RequestParams params,SearchRequest request){
        //构建布尔查询
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();

        //关键字搜索
        String key = params.getKey();
        if("".equals(key)||key==null){
            boolQuery.must(QueryBuilders.matchAllQuery());
        }else {
            boolQuery.must(QueryBuilders.matchQuery("all",key));
        }

        //条件过滤

        //城市
        if(params.getCity()!=null&&!params.getCity().equals("")){
            boolQuery.filter(QueryBuilders.termQuery("city",params.getCity()));
        }
        //品牌
        if(params.getBrand()!=null&&!params.getBrand().equals("")){
            boolQuery.filter(QueryBuilders.termQuery("brand",params.getBrand()));
        }
        //星级
        if(params.getStarName()!=null&&!params.getStarName().equals("")){
            boolQuery.filter(QueryBuilders.termQuery("starName",params.getStarName()));
        }

        //价格
        if(params.getMinPrice()!=null&&params.getMaxPrice()!=null){
            boolQuery.filter(QueryBuilders.rangeQuery("price").gte(params.getMinPrice())
                    .lte(params.getMaxPrice()));
        }

        //算分控制
        FunctionScoreQueryBuilder functionScoreQuery = QueryBuilders.functionScoreQuery
                (boolQuery,//原始查询
                        //function score数组
                        new FunctionScoreQueryBuilder.FilterFunctionBuilder[]{
                                //具体算分函数
                                new FunctionScoreQueryBuilder.FilterFunctionBuilder(
                                        //过滤条件 满足条件才算分
                                        QueryBuilders.termQuery("isAD",true),
                                        //算分函数 默认乘10
                                        ScoreFunctionBuilders.weightFactorFunction(10)
                                )
                        });

        request.source().query(functionScoreQuery);
    }

    private List<String> getAggByName(Aggregations aggregations,String aggName){
        //根据聚合名称获取聚合结果

       Terms terms = aggregations.get(aggName);

        List<? extends Terms.Bucket> buckets = terms.getBuckets();

        List<String> list = new ArrayList<>();

        for (Terms.Bucket bucket : buckets){
            String s = bucket.getKeyAsString();
            list.add(s);
        }

        return list;
    }

    @Override
    public List<String> getSuggestions(String prefix) {
        try {
            SearchRequest request = new SearchRequest("hotel");

            request.source().suggest(new SuggestBuilder().addSuggestion(
               "suggestions",
                    SuggestBuilders.completionSuggestion("suggestion")
                    .prefix(prefix)
                    .skipDuplicates(true)
                    .size(10)
            ));

            SearchResponse response = client.search(request, RequestOptions.DEFAULT);

            Suggest suggest = response.getSuggest();

            CompletionSuggestion suggestions = suggest.getSuggestion("suggestions");

            List<CompletionSuggestion.Entry.Option> options = suggestions.getOptions();

            List<String> list = new ArrayList<>(options.size());
            for (CompletionSuggestion.Entry.Option option:options){
                String text = option.getText().toString();
                list.add(text);
            }
            return list;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void deleteById(Long id) {

        try {
            DeleteRequest request = new DeleteRequest("hotel", id.toString());

            client.delete(request,RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }


    }

    @Override
    public void insertById(Long id) {
        try {
            Hotel hotel = hotelService.getById(id);

            HotelDoc hotelDoc = new HotelDoc(hotel);

            IndexRequest request = new IndexRequest("hotel").id(hotel.getId().toString());

            request.source(JSON.toJSONString(hotelDoc), XContentType.JSON);

            client.index(request,RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }


    }
}
