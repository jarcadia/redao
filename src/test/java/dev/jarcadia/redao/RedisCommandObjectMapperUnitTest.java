package dev.jarcadia.redao;

import java.io.IOException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import com.fasterxml.jackson.databind.ObjectMapper;

@ExtendWith(MockitoExtension.class)
public class RedisCommandObjectMapperUnitTest {
	
	@Mock
	RedaoCommando rcommando;
	
	@Mock
    Index index;
	
	@Mock
	Dao dao;
	
	@BeforeEach
	public void setupRcommando() {
    	Mockito.when(rcommando.getPrimaryIndex("objs")).thenReturn(index);
	}

	@BeforeEach
	public void setupDaoSet() {
    	Mockito.when(index.get("abc123")).thenReturn(dao);
	}

	@BeforeEach
	public void setupDao() {
    	Mockito.when(dao.getType()).thenReturn("objs");
    	Mockito.when(dao.getId()).thenReturn("abc123");
		Mockito.when(dao.getPath()).thenReturn("objs/abc123");
	}

    @Test
    void basicDao() throws IOException {
    	ObjectMapper mapper = new RedaoObjectMapper(rcommando);
    	String serialized = mapper.writeValueAsString(dao);
    	Assertions.assertEquals(quoted("objs/abc123"), serialized);
    	Dao dao = mapper.readValue(serialized, Dao.class);
    	Assertions.assertEquals("objs", dao.getType());
    	Assertions.assertEquals("abc123", dao.getId());
		Assertions.assertEquals("objs/abc123", dao.getPath());
	}
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    private String quoted(String str) {
    	return '"' + str + '"';
    }
}
