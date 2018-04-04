# First Of All
这是一个代码经过精简的入门版本，主要包括三部分内容:
1. src/scala/lite/main/ContructTpin:Mining suspicious tax evasion groups in big data中的TPIN构建代码
2. src/scala/lite/main/ContructInfn:只包含企业纳税人的影响关系网络构建方法
3. src/scala/lite/main/InfluenceMeasure:影响关系网络的直接影响度量和间接影响度量
# 其他文件夹说明
1. src/java/utils/Parameters:从/src/resources/oracle.properties文件读取Oracle账号的代码
2. src/scala/lite/entity:对各网络的点边进行封闭的实体类
3. src/scala/lite/utils:访问外部存储HDFS或者ORACLE的代码
# 核心方法说明
### ContructTpin.getFromOracleTable
共120行，使用DataFrame读Oracle，使用GraphX构图
### ContructInfn.getGraph
共20行，getOrCompute方式，依次调用ContructTpin.getFromOracleTable和ContructInfn.addil
### InfluenceMeasure.computeInfluencePro
共10行，依次调用_getOrComputeInflu、_getOrComputePaths、_influenceOnPath
