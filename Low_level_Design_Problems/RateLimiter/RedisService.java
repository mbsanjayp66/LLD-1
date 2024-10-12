package RateLimiter;

import java.time.Duration;
import java.time.Instant;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class RedisService {
	private int rateLimitPerMinute;
	private Map<String, Map<Instant,Integer>> userIdToRequest;
    public RedisService(int rpm) {
        rateLimitPerMinute = rpm;
        userIdToRequest = new ConcurrentHashMap<>();
    }
    
    public boolean addNewUser(String userName, Instant requestedTs) {
        Map<Instant,Integer>userRequestMap = new ConcurrentHashMap<>();
        userRequestMap.putIfAbsent(requestedTs, userRequestMap.getOrDefault(requestedTs, 0)+1);
        userIdToRequest.put(userName, userRequestMap);
        System.out.println("New User !!! - " + userName);
        return true;
    }
    
    public synchronized boolean requestHit(String userName, Instant ts) {

        if (!userIdToRequest.containsKey(userName)) {
            return addNewUser(userName, ts);
        } else {
        	Map<Instant, Integer>userRequestMap = userIdToRequest.get(userName);
        	for(Instant instant:userRequestMap.keySet()) {
        		Duration duration = Duration.between(instant, ts);
        		if (duration.getSeconds() >= 60) {
        			userRequestMap.remove(instant);
                } 
        	}
        	if(userRequestMap.values().stream().mapToInt(X->X).sum()>=rateLimitPerMinute) {
        		System.out.println("Request is Declined");
        		return false;
        	}
        	userRequestMap.putIfAbsent(ts, userRequestMap.getOrDefault(ts, 0)+1);
        	userIdToRequest.put(userName, userRequestMap);
        	System.out.println("Request is Accepted");
        	return true;
        }
    }
    
}
