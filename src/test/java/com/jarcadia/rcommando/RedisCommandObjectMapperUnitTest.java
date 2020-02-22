package com.jarcadia.rcommando;

import java.io.IOException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

@ExtendWith(MockitoExtension.class)
public class RedisCommandObjectMapperUnitTest {
	
	@Mock
	RedisCommando rcommando;
	
	@Mock
	DaoSet daoSet;
	
	@Mock
	Dao dao;
	
	@BeforeEach
	public void setupRcommando() {
    	Mockito.when(rcommando.getSetOf("objs")).thenReturn(daoSet);
	}

	@BeforeEach
	public void setupDaoSet() {
    	Mockito.when(daoSet.get("abc123")).thenReturn(dao);
	}

	@BeforeEach
	public void setupDao() {
    	Mockito.when(dao.getSetKey()).thenReturn("objs");
    	Mockito.when(dao.getId()).thenReturn("abc123");
	}

    @Test
    void basicDao() throws IOException {
    	ObjectMapper mapper = new RedisCommandoObjectMapper(rcommando);
    	String serialized = mapper.writeValueAsString(dao);
    	Assertions.assertEquals(quoted("objs:abc123"), serialized);
    	Dao dao = mapper.readValue(serialized, Dao.class);
    	Assertions.assertEquals("objs", dao.getSetKey());
    	Assertions.assertEquals("abc123", dao.getId());
    }
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    private String quoted(String str) {
    	return '"' + str + '"';
    }
}
