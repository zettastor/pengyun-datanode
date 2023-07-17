The DataNode module is deployed on each storage node server and serves as the actual manager of physical storage resources and the executor of data read and write operations. The DataNode module is responsible for the following tasks:

- Identifying and managing the hard disks within the storage node server, formatting the disks according to custom data structures, and managing the usage of storage space on each disk.

- Receiving read and write requests distributed by the [Coordinator](https://github.com/zettastor/pengyun-coordinator) and performing data read and write operations.

- Maintaining the replication relationship between local data and data on other DataNodes, and performing tasks such as master-slave replica switching and data reconstruction in case of failures.