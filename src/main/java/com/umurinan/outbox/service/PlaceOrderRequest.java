package com.umurinan.outbox.service;

import java.math.BigDecimal;

public record PlaceOrderRequest(String userId, BigDecimal total) {
}
