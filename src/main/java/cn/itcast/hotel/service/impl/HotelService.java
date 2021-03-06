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
            //1.??????request
            SearchRequest request = new SearchRequest("hotel");

            //2.??????DSL
            //2.1??????
            buildBasicQuery(params,request);

            //2.2??????
            int page = params.getPage();
            int size = params.getSize();
            request.source().from((page-1)*size).size(size);

            //2.3??????
            String location = params.getLocation();
            if(location!=null&&!location.equals("")){
                request.source().sort(SortBuilders.geoDistanceSort("location",
                        new GeoPoint(location)).
                        order(SortOrder.ASC)
                        .unit(DistanceUnit.KILOMETERS)
                );
            }

            //3.????????????
            SearchResponse response = client.search(request, RequestOptions.DEFAULT);

            //4.????????????
            return handleResponse(response);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Map<String, List<String>> getFilters(RequestParams params) {

        try {
            //1.??????request
            SearchRequest request = new SearchRequest("hotel");

            //2.??????DSL
            //??????Query
            buildBasicQuery(params,request);

            request.source().size(0);
            //??????
            buildAggregation(request);

            //3.????????????
            SearchResponse response = client.search(request, RequestOptions.DEFAULT);

            //4.????????????
            Map<String, List<String>> result = new HashMap<>();
            Aggregations aggregations = response.getAggregations();

            List<String> brandList = getAggByName(aggregations, "brandAgg");
            result.put("??????",brandList);
            List<String> cityList = getAggByName(aggregations, "cityAgg");
            result.put("??????",cityList);
            List<String> starList = getAggByName(aggregations, "starAgg");
            result.put("??????",starList);

            return result;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }


    }



    /**
     * ????????? ?????? ??????????????????
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
        //????????????
        SearchHits searchHits = response.getHits();

        //4.1???????????????
        long total = searchHits.getTotalHits().value;
        System.out.println("????????????"+total+"?????????");
        //4.2????????????
        SearchHit[] hits = searchHits.getHits();
        List<HotelDoc> hotels = new ArrayList<>();
        //4.3??????
        for (SearchHit hit:hits){
            //????????????source
            String json = hit.getSourceAsString();
            //????????????
            HotelDoc hotelDoc = JSON.parseObject(json, HotelDoc.class);

            //???????????????
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
     * ???????????? ?????????????????? ?????????????????? ?????????????????????must??? ????????????
     * ?????????????????????filter??? ??????????????? ???Query??????request???
     * @param params
     * @param request
     */
    private void buildBasicQuery(RequestParams params,SearchRequest request){
        //??????????????????
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();

        //???????????????
        String key = params.getKey();
        if("".equals(key)||key==null){
            boolQuery.must(QueryBuilders.matchAllQuery());
        }else {
            boolQuery.must(QueryBuilders.matchQuery("all",key));
        }

        //????????????

        //??????
        if(params.getCity()!=null&&!params.getCity().equals("")){
            boolQuery.filter(QueryBuilders.termQuery("city",params.getCity()));
        }
        //??????
        if(params.getBrand()!=null&&!params.getBrand().equals("")){
            boolQuery.filter(QueryBuilders.termQuery("brand",params.getBrand()));
        }
        //??????
        if(params.getStarName()!=null&&!params.getStarName().equals("")){
            boolQuery.filter(QueryBuilders.termQuery("starName",params.getStarName()));
        }

        //??????
        if(params.getMinPrice()!=null&&params.getMaxPrice()!=null){
            boolQuery.filter(QueryBuilders.rangeQuery("price").gte(params.getMinPrice())
                    .lte(params.getMaxPrice()));
        }

        //????????????
        FunctionScoreQueryBuilder functionScoreQuery = QueryBuilders.functionScoreQuery
                (boolQuery,//????????????
                        //function score??????
                        new FunctionScoreQueryBuilder.FilterFunctionBuilder[]{
                                //??????????????????
                                new FunctionScoreQueryBuilder.FilterFunctionBuilder(
                                        //???????????? ?????????????????????
                                        QueryBuilders.termQuery("isAD",true),
                                        //???????????? ?????????10
                                        ScoreFunctionBuilders.weightFactorFunction(10)
                                )
                        });

        request.source().query(functionScoreQuery);
    }

    private List<String> getAggByName(Aggregations aggregations,String aggName){
        //????????????????????????????????????

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
