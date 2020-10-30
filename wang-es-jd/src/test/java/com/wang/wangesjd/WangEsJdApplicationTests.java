package com.wang.wangesjd;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
class WangEsJdApplicationTests {

	@Test
	void contextLoads() {
		int x,y,z;
		for (x = 1; x < 9; x++) {
			for (y = 1; y < 9; y++) {
				for (z = 1; z < 9; z++) {
					if ((100 * x + 10 * y + z) + (100 * z + 10 * y + x) == 1231)
						System.out.println(x + " " + y + " " + z);
				}
			}


		}
	}

}
