package org.praveen.s3selectdemo.controller;

import org.praveen.s3selectdemo.service.GetItemUseCase;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class TestController {

    private final GetItemUseCase getItemsUseCase;

    public TestController(GetItemUseCase getItemsUseCase) {
        this.getItemsUseCase = getItemsUseCase;
    }

    @GetMapping("/api/test")
    public String testApi() {
        return "Hello World";
    }

    @GetMapping("/api/getCustomer")
    @ResponseStatus(code = HttpStatus.BAD_REQUEST)
    public ResponseEntity<Object> getCustomerReturn(@RequestParam String field, String value) {
        return ResponseEntity.ok(getItemsUseCase.getItems(field, value));
    }
}
