package com.umurinan.outbox.service;

import java.math.BigDecimal;

public record OrderResponse(Long orderId, String userId, BigDecimal total, String status) {
}
