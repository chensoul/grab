package com.javachen.grab.model;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class CommonResponse<T> {
	private Boolean success;
	private String message;
	private T data;
	
	public static <T> CommonResponse<T> success(){
		return success(null);
	}
	
	public static <T> CommonResponse<T> success(T data){
		return new CommonResponse<T>(true,"ok",data);
	}

	public static <T> CommonResponse<T> error(){
		return new CommonResponse<T>(false,"fail",null);
	}

	public static <T> CommonResponse<T> error(String message){
		return new CommonResponse<T>(false,message,null);
	}
}
