﻿# kafkaProducer2

这次修改包含：
1.consumer对于提交offsets的处理
2.生产者的key改为tags+timestamp，topic为meansurement
3.在生产者发送record时使用timestamp对分区数取模作为分发到partition的依据
4.influxdb的获取改为单例模式


new:
1.在程序正常运行时采用map维护partition和offset的关系，若程序程序出现异常再次启动时若map为空，则读取kafka中的信息，若kafka中不存在，则去文件中读取，若文件中还是不存在，则offset从0开始。
2.修改了写文件和读文件方法