# db-engine

这是吉林大学软件工程卓越工程师班《*数据库系统应用程序开发*》和《*系统软件综合实践*》两门课程（共 3 学分）综合起来共同完成的课程项目。课程的内容主要是了解 [Stanford CS346 课程](https://web.stanford.edu/class/cs346/2015/redbase-pf.html)的 RedBase 框架，仿照实现一个**数据库引擎**的部分功能。

使用 [Rust](https://rust-lang.org/) 实现。

课程要求实现的数据库主要有**四大功能**，具体介绍可以参考我[**博客**](https://blog.kisechan.space/)中的文章，文章中也提供了对应部分的**课程具体要求**，每一部分我也给出了对应的**测试代码**（在 `./src/test/` 路径下）：
1. [**内存、外存和记录管理**](https://blog.kisechan.space/2025/db-engine-1/)，对应 [task1](./src/test/task1.rs) 和 [*task2*](./src/test/task2.rs)（*没有要求做实验验证，所以没有对应代码*）。
2. [**索引**](https://blog.kisechan.space/2025/db-engine-2/)，对应 [task3](./src/test/task3.rs)。
3. [**SQL 处理**](https://blog.kisechan.space/2025/db-engine-3/)，对应 [task4](./src/test/task4.rs)。