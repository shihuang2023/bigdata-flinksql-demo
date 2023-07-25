package com.flinkcdc;

import com.flinkcdc.job.Mysql2Es;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * @Description TODO
 * @Author zwy
 * @Date 2023/7/24 16:44
 * @Version 1.0
 */
@SpringBootApplication
public class FlinkCdcApplication {

    public static void main(String[] args) {
        SpringApplication.run(FlinkCdcApplication.class,args);
        Mysql2Es mysql2Es = new Mysql2Es();
        mysql2Es.startJob();

    }

}
