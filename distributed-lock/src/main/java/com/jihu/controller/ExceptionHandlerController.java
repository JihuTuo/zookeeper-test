package com.jihu.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;


@ControllerAdvice
@Slf4j
public class ExceptionHandlerController {


    @ExceptionHandler
    @ResponseStatus(value = HttpStatus.BAD_REQUEST)
    @ResponseBody
    public Object exceptionHandler(RuntimeException e){
        log.error("ExceptionHandlerController: message:{}. stackTrace: {}", e.getMessage(), e.getStackTrace());
        Map<String,Object> result=new HashMap<>(  );
        result.put( "status","error" );
        result.put( "message",e.getMessage() );
        return result;
    }


}
