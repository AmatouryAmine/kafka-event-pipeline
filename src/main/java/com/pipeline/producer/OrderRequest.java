package com.pipeline.producer;

import lombok.Data;

import java.math.BigDecimal;

@Data
public class OrderRequest {

    private String customerId;
    private String product;
    private BigDecimal amount;
}
