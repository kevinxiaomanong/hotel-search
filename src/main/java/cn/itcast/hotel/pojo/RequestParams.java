package cn.itcast.hotel.pojo;


import lombok.Data;

@Data
public class RequestParams {

    private String key;
    private Integer page;
    private Integer size;
    private Integer minPrice;
    private Integer maxPrice;
    private String sortBy;
    private String city;
    private String brand;
    private String starName;
    private String location;


}
