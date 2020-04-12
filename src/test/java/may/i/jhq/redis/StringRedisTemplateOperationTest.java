package may.i.jhq.redis;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.util.Assert;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@SpringBootTest
public class StringRedisTemplateOperationTest {

	@Autowired
	private StringRedisTemplate redisTemplate;

	@BeforeEach
	public void init() {
		redisTemplate.opsForValue().set("key1", "value1");
		redisTemplate.opsForList().leftPushAll("list1", "element1", "element2");
		redisTemplate.opsForHash().put("may", "iphone", "6");
		redisTemplate.opsForHash().put("may", "samsung", "s10");
		redisTemplate.opsForSet().add("apple", "iphone6", "iphone7", "iphone8");
		redisTemplate.opsForZSet().add("zapple", "iphone6", 6);
		redisTemplate.opsForZSet().add("zapple", "iphone7", 7);
		redisTemplate.opsForZSet().add("zapple", "iphone8", 8);
		redisTemplate.opsForGeo().add("zhejiang", new Point(120.19D, 30.26D), "hangzhou");
	}

	@Test
	public void getOperation() {
		String value1 = redisTemplate.opsForValue().get("key1");
		Assert.isTrue(Objects.equals(value1, "value1"), "Key-Value Get Error");
		List<String> list1 = redisTemplate.opsForList().range("list1", 0, -1);
		Assert.isTrue(list1.size() > 0, "List Get Error");
		String element1 = redisTemplate.opsForList().rightPop("list1");
		String element2 = redisTemplate.opsForList().rightPop("list1");

		Assert.isTrue(Objects.equals(element1, "element1"), "List Pop Error");
		Assert.isTrue(Objects.equals(element2, "element2"), "List Pop Error");
		Map<Object, Object> iphoneVersionMap = redisTemplate.opsForHash().entries("may");
		Assert.isTrue(iphoneVersionMap.containsKey("iphone"), "May  does'n have iphone.");
		Assert.isTrue(Objects.equals(iphoneVersionMap.get("samsung").toString(), "s10"), "May does'n have S10.");
		Set<String> iphoneSet = redisTemplate.opsForSet().members("apple");
		Assert.isTrue(iphoneSet.size() > 0, "Iphone set is empty");
		List<String> iphoneList = redisTemplate.opsForSet().pop("apple", 2);
		int leftIphoneSize = iphoneSet.size() - iphoneList.size();
		Assert.isTrue(redisTemplate.opsForSet().size("apple") == leftIphoneSize, "Iphone still have a set");

		Long count = redisTemplate.opsForZSet().count("zapple", 6, 7);
		Assert.isTrue(count == 2, "Count Error");
		Set<String> iphoneZSet = redisTemplate.opsForZSet().range("zapple", 0, 1);
		Assert.isTrue(iphoneZSet.size() == 2, "Iphone count error");
		List<Point> position = redisTemplate.opsForGeo().position("zhejiang", "hangzhou");
		Assert.isTrue(position.size() == 1, "Hangzhou' point doesn't exist");
	}

	@AfterEach
	public void removeOperation() throws InterruptedException {
		redisTemplate.delete("key1");
		Assert.isTrue(!redisTemplate.hasKey("key1"), "Deleted Key Error");
		redisTemplate.delete("list1");
		Assert.isTrue(!redisTemplate.hasKey("list1"), "List is Not Empty");
		redisTemplate.opsForHash().delete("may", "iphone", "samsung");
		Assert.isTrue(redisTemplate.opsForHash().size("may") == 0, "May still has device.");
		redisTemplate.delete("may");
		Assert.isTrue(!redisTemplate.hasKey("may"), "May still alive");
		redisTemplate.boundSetOps("apple").expire(10, TimeUnit.MICROSECONDS);
		Thread.sleep(10);
		Assert.isTrue(!redisTemplate.hasKey("apple"), "Iphone is expired");
		Long removeCount = redisTemplate.opsForZSet().remove("zapple", "iphone6", "iphone7");
		redisTemplate.boundZSetOps("zapple").expire(10, TimeUnit.MICROSECONDS);
		Thread.sleep(10);
		Assert.isTrue(!redisTemplate.hasKey("apple") && removeCount == 2, "Iphone is removed");
		Long count = redisTemplate.opsForGeo().remove("zhejiang", "hangzhou");
		Assert.isTrue(count == 1, "Rm Success");
		redisTemplate.boundGeoOps("zhejiang").expire(10, TimeUnit.MICROSECONDS);
		Thread.sleep(10);
		Assert.isTrue(!redisTemplate.hasKey("zhejiang"), "Key still existed");
	}
}
