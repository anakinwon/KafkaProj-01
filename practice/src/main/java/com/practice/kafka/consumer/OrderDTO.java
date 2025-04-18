package com.practice.kafka.consumer;

import java.time.LocalDateTime;

/*  <테이블 생성>
    CREATE TABLE orders
       ( ord_id varchar(10)
       , shop_id varchar(10)
       , menu_name varchar(100)
       , user_name varchar(100)
       , phone_number varchar(100)
       , address varchar(200)
       , order_time timestamp
       );
*/

public class OrderDTO {
    public String orderId;
    public String shopId;
    public String menuName;
    public String userName;
    public String phoneNumber;
    public String address;
    public LocalDateTime orderTime;

    public OrderDTO(String orderId, String shopId, String menuName, String userName,
                    String phoneNumber, String address, LocalDateTime orderTime) {
        this.orderId = orderId;
        this.shopId = shopId;
        this.menuName = menuName;
        this.userName = userName;
        this.phoneNumber = phoneNumber;
        this.address = address;
        this.orderTime = orderTime;
    }
}